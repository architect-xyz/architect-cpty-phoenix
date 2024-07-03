use crate::ConnectionCtx;
use anyhow::{Context, Result};
use api::{cpty::generic_external::ExternalBookSnapshot, DirPair};
use chrono::Utc;
use fxhash::FxHashSet;
use governor::{
    clock::DefaultClock,
    state::{InMemoryState, NotKeyed},
    RateLimiter,
};
use hdrhistogram::Histogram;
use itertools::Itertools;
use log::{debug, error, warn};
use num_traits::FromPrimitive;
use phoenix::quantities::WrapperU64;
use phoenix_sdk::sdk_client::SDKClient;
use phoenix_sdk_core::orderbook::{OrderbookKey, OrderbookValue};
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use solana_sdk::pubkey::Pubkey;
use std::{
    collections::{HashMap, HashSet},
    sync::{atomic::Ordering, Arc},
};

pub async fn poll_market(
    sdk_client: Arc<SDKClient>,
    rpc_limiter: &RateLimiter<NotKeyed, InMemoryState, DefaultClock>,
    ctx: &ConnectionCtx,
    trader: Pubkey,
    prioritization_fee_percentile: f64,
) -> Result<()> {
    let mut tokens = FxHashSet::default();
    let mut balances: HashMap<String, Decimal> = HashMap::new();
    for (_, mi) in &ctx.market_info {
        // poll the prio fee while we're here
        rpc_limiter.until_ready().await;
        let mut recent_fees = Histogram::<u64>::new(3)?;
        let rpfs = sdk_client.client.get_recent_prioritization_fees(&[mi.pubkey]).await?;
        for rpf in &rpfs {
            if rpf.prioritization_fee != 0 {
                if let Err(e) = recent_fees.record(rpf.prioritization_fee) {
                    error!("recording prioritization fee: {e}");
                }
            }
        }
        let median_fee = if recent_fees.is_empty() {
            0u64
        } else {
            recent_fees.value_at_percentile(prioritization_fee_percentile)
        };
        debug!("median prioritization fee for {}: {}", mi.pubkey, median_fee);
        mi.prioritization_fee.store(median_fee, Ordering::Relaxed);
        tokens.insert(mi.base.clone());
        tokens.insert(mi.quote.clone());
        rpc_limiter.until_ready().await;
        let state = sdk_client.get_market_state(&mi.pubkey).await?;
        let convert_price_size = |(price, size): (f64, f64)| match (
            Decimal::from_f64(price),
            Decimal::from_f64(size),
        ) {
            (Some(price), Some(size)) => Some((price, size)),
            _ => {
                error!("unrepresentable price or size while parsing book");
                None
            }
        };
        let bids: Vec<(Decimal, Decimal)> = state
            .orderbook
            .get_bids()
            .iter()
            .rev()
            .chunk_by(|(k, _)| {
                k.price() * state.orderbook.quote_units_per_raw_base_unit_per_tick
            })
            .into_iter()
            .filter_map(|(p, g)| {
                let size = g.map(|(_, o)| o.size()).sum::<f64>();
                convert_price_size((p, size))
            })
            .collect();
        let asks: Vec<(Decimal, Decimal)> = state
            .orderbook
            .get_asks()
            .iter()
            .chunk_by(|(k, _)| {
                k.price() * state.orderbook.quote_units_per_raw_base_unit_per_tick
            })
            .into_iter()
            .filter_map(|(p, g)| {
                let size = g.map(|(_, o)| o.size()).sum::<f64>();
                convert_price_size((p, size))
            })
            .collect();
        {
            let mut book_snapshot = mi.book_snapshot.lock().unwrap();
            *book_snapshot = Some(ExternalBookSnapshot {
                book: DirPair { buy: Arc::new(bids), sell: Arc::new(asks) },
                timestamp: Utc::now(),
            });
        }
        if let Some(tstate) = state.traders.get(&trader) {
            // CR alee: support locked balances in external cpty protocol;
            // not sure if its per market or they share the same vaults.
            let base_entry = balances.entry(mi.base.name.clone()).or_insert(dec!(0));
            let base_amt_free = Decimal::from_i128_with_scale(
                mi.metadata.base_lots_to_base_atoms(tstate.base_lots_free.as_u64())
                    as i128,
                mi.base.decimals as u32,
            )
            .normalize();
            let base_amt_locked = Decimal::from_i128_with_scale(
                mi.metadata.base_lots_to_base_atoms(tstate.base_lots_locked.as_u64())
                    as i128,
                mi.base.decimals as u32,
            )
            .normalize();
            *base_entry += base_amt_free;
            *base_entry += base_amt_locked;
            let quote_entry = balances.entry(mi.quote.name.clone()).or_insert(dec!(0));
            let quote_amt_free = Decimal::from_i128_with_scale(
                mi.metadata.quote_lots_to_quote_atoms(tstate.quote_lots_free.as_u64())
                    as i128,
                mi.quote.decimals as u32,
            )
            .normalize();
            let quote_amt_locked = Decimal::from_i128_with_scale(
                mi.metadata.quote_lots_to_quote_atoms(tstate.quote_lots_locked.as_u64())
                    as i128,
                mi.quote.decimals as u32,
            )
            .normalize();
            *quote_entry += quote_amt_free;
            *quote_entry += quote_amt_locked;
            // we must have a seat if we have a tstate
            mi.seated.store(true, Ordering::Relaxed);
        } else {
            // we must have been evicted
            mi.seated.store(false, Ordering::Relaxed);
        }
        {
            let mut oo = mi.open_oids.lock().unwrap();
            let mut oids_seen: HashSet<u64> = HashSet::new();
            for (foid, order) in &state.orderbook.bids {
                if order.maker_id == trader {
                    let osn = foid.order_sequence_number;
                    if !oo.contains_key(&osn) {
                        warn!("found bid order #{osn} not known to Architect");
                    } else {
                        debug!("found bid order #{osn}: {:?}", order);
                    }
                    oids_seen.insert(osn);
                }
            }
            for (foid, order) in &state.orderbook.asks {
                if order.maker_id == trader {
                    let osn = foid.order_sequence_number;
                    if !oo.contains_key(&osn) {
                        warn!("found ask order #{osn} not known to Architect");
                    } else {
                        debug!("found ask order #{osn}: {:?}", order);
                    }
                    oids_seen.insert(osn);
                }
            }
            oo.retain(|k, _| oids_seen.contains(k));
        }
    }
    for ti in &tokens {
        let entry = balances.entry(ti.name.clone()).or_insert(dec!(0));
        for ata in &ti.atas {
            rpc_limiter.until_ready().await;
            let amt = sdk_client
                .client
                .get_token_account_balance(&ata)
                .await
                .with_context(|| format!("getting balance for {}", ata))?;
            let dec_amt =
                Decimal::from_i128_with_scale(amt.amount.parse()?, ti.decimals as u32)
                    .normalize();
            *entry += dec_amt;
        }
    }
    rpc_limiter.until_ready().await;
    let bal = sdk_client.client.get_balance(&trader).await?;
    let dec_amt = Decimal::from_i128_with_scale(bal as i128, 9).normalize();
    let entry = balances.entry("SOL Crypto".to_string()).or_insert(dec!(0));
    *entry += dec_amt;
    println!("{balances:?}");
    {
        let mut ctx_balances = ctx.balances.lock().unwrap();
        *ctx_balances = balances;
    }
    Ok(())
}
