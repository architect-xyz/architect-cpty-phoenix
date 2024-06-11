use crate::ConnectionCtx;
use anyhow::{Context, Result};
use fxhash::FxHashSet;
use governor::{
    clock::DefaultClock,
    state::{InMemoryState, NotKeyed},
    RateLimiter,
};
use log::{debug, warn};
use phoenix::quantities::WrapperU64;
use phoenix_sdk::sdk_client::SDKClient;
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use solana_sdk::pubkey::Pubkey;
use std::{
    collections::{HashMap, HashSet},
    sync::{atomic::Ordering, Arc},
};

pub async fn poll_balances_and_open_orders(
    sdk_client: Arc<SDKClient>,
    rpc_limiter: &RateLimiter<NotKeyed, InMemoryState, DefaultClock>,
    ctx: &ConnectionCtx,
    trader: Pubkey,
) -> Result<()> {
    let mut tokens = FxHashSet::default();
    let mut balances: HashMap<String, Decimal> = HashMap::new();
    for (_, mi) in &ctx.market_info {
        // poll the prio fee while we're here
        rpc_limiter.until_ready().await;
        let mut recent_fees = vec![];
        let rpfs = sdk_client.client.get_recent_prioritization_fees(&[mi.pubkey]).await?;
        for rpf in &rpfs {
            if rpf.prioritization_fee != 0 {
                recent_fees.push(rpf.prioritization_fee);
            }
        }
        let median_fee = if recent_fees.is_empty() {
            0u64
        } else {
            recent_fees.sort();
            recent_fees[recent_fees.len() / 2]
        };
        debug!("median prioritization fee for {}: {}", mi.pubkey, median_fee);
        mi.prioritization_fee.store(median_fee, Ordering::Relaxed);
        tokens.insert(mi.base.clone());
        tokens.insert(mi.quote.clone());
        rpc_limiter.until_ready().await;
        let state = sdk_client.get_market_state(&mi.pubkey).await?;
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
