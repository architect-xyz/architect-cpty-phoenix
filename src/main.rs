use anyhow::{anyhow, bail, Context, Result};
use api::{
    cpty::generic_external::*,
    orderflow::{Ack, FillId, FillKind, OrderType, Out, TimeInForce},
    symbology::VenueId,
    Dir, HumanDuration,
};
use architect_cpty_phoenix::*;
use bytes::{BufMut, BytesMut};
use chrono::{TimeZone, Utc};
use clap::Parser;
use ellipsis_client::EllipsisClient;
use futures_util::{select_biased, FutureExt, SinkExt, StreamExt};
use fxhash::FxHashSet;
use governor::{
    clock::DefaultClock,
    state::{InMemoryState, NotKeyed},
    Quota, RateLimiter,
};
use log::{debug, error, info, trace, warn};
use num_traits::{cast::ToPrimitive, FromPrimitive};
use phoenix::state::{markets::FIFOOrderId, SelfTradeBehavior, Side};
use phoenix_sdk::{
    order_packet_template::{LimitOrderTemplate, PostOnlyOrderTemplate},
    sdk_client::SDKClient,
};
use phoenix_sdk_core::market_event::MarketEventDetails;
use rand::Rng;
use rust_decimal::Decimal;
use serde::Deserialize;
use serde_json::json;
use solana_client::{
    nonblocking::rpc_client::RpcClient,
    rpc_client::GetConfirmedSignaturesForAddress2Config,
    rpc_request::TokenAccountsFilter,
};
use solana_sdk::{
    commitment_config::CommitmentConfig,
    pubkey::Pubkey,
    signature::{Keypair, Signature},
    signer::Signer,
};
use std::{
    collections::{BTreeMap, HashMap},
    net::SocketAddr,
    num::NonZeroU32,
    path::PathBuf,
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc, Mutex,
    },
};
use tokio::{
    net::{TcpListener, TcpStream},
    sync::broadcast,
    time::MissedTickBehavior,
};
use tokio_tungstenite::{tungstenite::Message, WebSocketStream};

#[derive(clap::Parser)]
struct Args {
    #[arg(long)]
    config: PathBuf,
    #[command(subcommand)]
    cmd: Command,
}

#[derive(clap::Subcommand)]
enum Command {
    DevnetAirdrop,
    Run,
}

#[derive(Debug, Deserialize)]
struct Config {
    rpc_url: String,
    rpc_rate_limit_per_sec: NonZeroU32,
    prioritization_fee_percentile: f64,
    venue: String,
    poll_markets_every: HumanDuration,
    markets: BTreeMap<String, MarketConfig>, // pubkey => MarketConfig
    tokens: BTreeMap<String, TokenConfig>,
}

#[derive(Debug, Deserialize)]
struct TokenConfig {
    name: String,
    decimals: u8,
}

#[derive(Debug, Clone, Deserialize)]
struct MarketConfig {
    tick_size: Decimal,
    step_size: Decimal,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    let config: Config = serde_json::from_str(&std::fs::read_to_string(&args.config)?)?;
    let markets = config
        .markets
        .iter()
        .map(|(k, v)| (k.parse().unwrap(), v.clone()))
        .collect::<BTreeMap<Pubkey, MarketConfig>>();
    let payer = utils::get_payer_keypair();
    env_logger::init();
    let solana_rpc_client = RpcClient::new_with_commitment(
        config.rpc_url.to_string(),
        CommitmentConfig::confirmed(),
    );
    let client = EllipsisClient::from_rpc(solana_rpc_client, &payer)?;
    // let sdk_client = SDKClient::new_from_ellipsis_client(client).await?;
    let sdk_client = SDKClient::new_from_ellipsis_client_with_all_markets(client).await?;
    match args.cmd {
        Command::DevnetAirdrop => todo!(),
        // Command::DevnetAirdrop => devnet_airdrop(sdk_client, payer, &markets[0]).await?,
        Command::Run => run(config, sdk_client, payer.pubkey(), markets).await?,
    }
    Ok(())
}

async fn devnet_airdrop(
    sdk_client: SDKClient,
    payer: Keypair,
    market: &Pubkey,
) -> Result<()> {
    sdk_client
        .client
        .request_airdrop(&payer.pubkey(), 1_000_000_000)
        .await
        .with_context(|| "requesting SOL airdrop")?;
    let ixs = utils::create_airdrop_spl_ixs(&sdk_client, market, &payer.pubkey())
        .await
        .ok_or_else(|| anyhow!("failed to create airdrop instructions"))?;
    sdk_client.client.sign_send_instructions(ixs, vec![]).await?;
    Ok(())
}

async fn get_token_info(
    config: &Config,
    sdk_client: &SDKClient,
    trader: Pubkey,
    token: Pubkey,
) -> Result<TokenInfo> {
    let tc = config
        .tokens
        .get(&token.to_string())
        .ok_or_else(|| anyhow!("token not found in config: {}", token))?;
    let ata_infos = sdk_client
        .client
        .get_token_accounts_by_owner(&trader, TokenAccountsFilter::Mint(token))
        .await?;
    let mut atas = vec![];
    for ata_info in ata_infos {
        atas.push(ata_info.pubkey.parse()?);
    }
    Ok(TokenInfo { pubkey: token, name: tc.name.clone(), decimals: tc.decimals, atas })
}

async fn run(
    config: Config,
    sdk_client: SDKClient,
    trader: Pubkey,
    markets: BTreeMap<Pubkey, MarketConfig>,
) -> Result<()> {
    let sdk_client = Arc::new(sdk_client);
    let rpc_limiter =
        Arc::new(RateLimiter::direct(Quota::per_second(config.rpc_rate_limit_per_sec)));
    let token_info: HashMap<Pubkey, TokenInfo> = HashMap::new();
    let mut market_info: HashMap<String, MarketInfo> = HashMap::new();
    for (market, market_config) in markets.iter() {
        debug!("loading market metadata for {}", market);
        rpc_limiter.until_ready().await;
        let metadata = sdk_client.get_market_metadata(&market).await?;
        let base = if let Some(ti) = token_info.get(&metadata.base_mint) {
            ti.clone()
        } else {
            get_token_info(&config, &sdk_client, trader, metadata.base_mint).await?
        };
        let quote = if let Some(ti) = token_info.get(&metadata.quote_mint) {
            ti.clone()
        } else {
            get_token_info(&config, &sdk_client, trader, metadata.quote_mint).await?
        };
        let market_name = format!("{}/{}", base.name, quote.name);
        market_info.insert(
            market_name,
            MarketInfo {
                pubkey: *market,
                tick_size: market_config.tick_size,
                step_size: market_config.step_size,
                prioritization_fee: Arc::new(AtomicU64::new(0)),
                metadata,
                base,
                quote,
                seated: Arc::new(AtomicBool::new(false)),
                open_oids: Mutex::new(Default::default()),
                book_snapshot: Mutex::new(None),
            },
        );
    }
    let (bcast_tx, _bcast_rx) = broadcast::channel::<ExternalCptyProtocol>(1000);
    let ctx = Arc::new(ConnectionCtx {
        market_info,
        balances: Mutex::new(Default::default()),
        open_cl_oids: Mutex::new(Default::default()),
        cloids: Mutex::new(Default::default()),
    });
    // initial balances and open orders sweep
    poll_market::poll_market(
        sdk_client.clone(),
        &rpc_limiter,
        &ctx,
        trader,
        config.prioritization_fee_percentile,
    )
    .await?;
    {
        let sdk_client = sdk_client.clone();
        let rpc_limiter = rpc_limiter.clone();
        let ctx = ctx.clone();
        tokio::task::spawn(async move {
            loop {
                if let Err(e) = poll_markets_task(
                    sdk_client.clone(),
                    rpc_limiter.clone(),
                    &ctx,
                    *config.poll_markets_every,
                    trader,
                    config.prioritization_fee_percentile,
                )
                .await
                {
                    error!("poll_balances_task: {:?}, restarting in 3s...", e);
                }
                tokio::time::sleep(std::time::Duration::from_secs(3)).await;
            }
        });
    }
    {
        let sdk_client = sdk_client.clone();
        let rpc_limiter = rpc_limiter.clone();
        let ctx = ctx.clone();
        let mut bcast_tx = bcast_tx.clone();
        let venue: VenueId = config.venue.parse()?;
        tokio::task::spawn(async move {
            for (_, mi) in &ctx.market_info {
                if let Err(e) = poll_transactions_task(
                    sdk_client.clone(),
                    rpc_limiter.clone(),
                    &ctx,
                    &mut bcast_tx,
                    venue,
                    mi.pubkey,
                    trader,
                )
                .await
                {
                    error!("poll_transactions_task: {:?}", e);
                }
            }
        });
    }
    let bind_addr: SocketAddr = "127.0.0.1:8000".parse()?;
    let listener = TcpListener::bind(&bind_addr).await?;
    info!("listening on {bind_addr}...");
    while let Ok((stream, _)) = listener.accept().await {
        let ctx = ctx.clone();
        let sdk_client = sdk_client.clone();
        let bcast_rx = bcast_tx.subscribe();
        tokio::spawn(async move {
            if let Err(e) =
                accept_connection(stream, ctx.clone(), sdk_client.clone(), bcast_rx).await
            {
                error!("connection: {e:?}");
            };
        });
    }
    Ok(())
}

async fn accept_connection(
    stream: TcpStream,
    ctx: Arc<ConnectionCtx>,
    sdk_client: Arc<SDKClient>,
    mut bcast_rx: broadcast::Receiver<ExternalCptyProtocol>,
) -> Result<()> {
    let peer_addr = stream.peer_addr()?;
    let mut ws = tokio_tungstenite::accept_async(stream)
        .await
        .with_context(|| "while websocket handshake")?;
    info!("accepted: {peer_addr}");
    loop {
        select_biased! {
            r = ws.next().fuse() => {
                if let Some(r) = r {
                    let msg = r?;
                    let msg: ExternalCptyProtocol = match msg {
                        Message::Text(data) => serde_json::from_str(&data)?,
                        Message::Binary(data) => serde_json::from_slice(&data)?,
                        Message::Ping(..) | Message::Pong(..) => continue,
                        Message::Frame(..) => {
                            bail!("unexpected ws frame");
                        }
                        Message::Close(close) => {
                            info!("closing {peer_addr}: {close:?}");
                            break;
                        }
                    };
                    handle_message(&mut ws, &ctx, msg, &sdk_client).await?;
                    ws.flush().await?;
                } else {
                    break;
                }
            }
            msg = bcast_rx.recv().fuse() => {
                let msg = msg?;
                let msg = serde_json::to_string(&msg)?;
                ws.send(Message::Text(msg)).await?;
            }
        }
    }
    Ok(())
}

async fn handle_message(
    ws: &mut WebSocketStream<TcpStream>,
    ctx: &ConnectionCtx,
    msg: ExternalCptyProtocol,
    sdk_client: &SDKClient,
) -> Result<()> {
    match msg {
        ExternalCptyProtocol::GetSymbology => {
            // reply with all known market names
            let mut json_markets: Vec<serde_json::Value> = vec![];
            for (market_name, market_info) in ctx.market_info.iter() {
                json_markets.push(json!({
                    "name": market_name,
                    "tick_size": market_info.tick_size,
                    "step_size": market_info.step_size,
                }));
            }
            let reply = json!({
                "type": "Symbology",
                "markets": json_markets
            });
            // let reply = ExternalCptyProtocol::Symbology(ExternalSymbology {
            //     markets: ctx.market_info.keys().cloned().collect(),
            // });
            let reply = serde_json::to_string(&reply)?;
            ws.send(Message::Text(reply)).await?;
        }
        ExternalCptyProtocol::GetOpenOrders => {
            let open_oids = {
                let open_cl_oids = ctx.open_cl_oids.lock().unwrap();
                let open_cl_oids: Vec<u128> = open_cl_oids.keys().cloned().collect();
                let cloids = ctx.cloids.lock().unwrap();
                open_cl_oids
                    .iter()
                    .filter_map(|cloid| cloids.get(cloid).copied())
                    .collect()
            };
            let reply =
                ExternalCptyProtocol::OpenOrders { open_orders: Arc::new(open_oids) };
            let reply = serde_json::to_string(&reply)?;
            ws.send(Message::Text(reply)).await?;
        }
        ExternalCptyProtocol::GetBalances { id } => {
            let balances: Vec<(String, Decimal)> = {
                let ctx_balances = ctx.balances.lock().unwrap();
                ctx_balances.iter().map(|(k, v)| (k.clone(), *v)).collect()
            };
            let reply = ExternalCptyProtocol::Balances {
                id,
                result: Some(ExternalBalances { balances }),
                error: None,
            };
            let reply = serde_json::to_string(&reply)?;
            ws.send(Message::Text(reply)).await?;
        }
        ExternalCptyProtocol::Order(o) => {
            macro_rules! reject {
                ($reason:expr) => {{
                    let reply = ExternalCptyProtocol::Reject(ExternalReject {
                        order_id: o.id,
                        reason: Some($reason.to_string()),
                    });
                    let reply = serde_json::to_string(&reply)?;
                    ws.send(Message::Text(reply)).await?;
                    return Ok(());
                }};
            }
            let market_info = match ctx.market_info.get(&o.market) {
                Some(entry) => entry,
                None => reject!("market not found"),
            };
            let market_key = &market_info.pubkey;
            let market_metadata = &market_info.metadata;
            let params = match o.order_type {
                OrderType::Limit(lot) => lot,
                _ => reject!("unsupported order type"),
            };
            let f_price = match params.limit_price.to_f64() {
                Some(p) => p,
                None => reject!("price decimals->f64 conversion error"),
            };
            let f_size = match o.quantity.to_f64() {
                Some(s) => s,
                None => reject!("size decimals->f64 conversion error"),
            };
            let side = match o.dir {
                Dir::Buy => Side::Bid,
                Dir::Sell => Side::Ask,
            };
            // client_order_id: lower 64 bits is the architect order id
            // upper 64 bits is a random seed to be able to identify it
            // on the stream later.
            let oid_seed: u64 = rand::thread_rng().gen();
            let client_oid = ((oid_seed as u128) << 64) | (o.id.seqno as u128);
            let ix = match o.time_in_force {
                TimeInForce::GoodTilCancel => {
                    if params.post_only {
                        sdk_client.get_post_only_ix_from_template(
                            &market_key,
                            &market_metadata,
                            &PostOnlyOrderTemplate {
                                side,
                                price_as_float: f_price,
                                size_in_base_units: f_size,
                                client_order_id: client_oid,
                                reject_post_only: true,
                                use_only_deposited_funds: false,
                                last_valid_slot: None,
                                last_valid_unix_timestamp_in_seconds: None,
                                fail_silently_on_insufficient_funds: false,
                            },
                        )?
                    } else {
                        sdk_client.get_limit_order_ix_from_template(
                            &market_key,
                            &market_metadata,
                            &LimitOrderTemplate {
                                side,
                                price_as_float: f_price,
                                size_in_base_units: f_size,
                                self_trade_behavior: SelfTradeBehavior::CancelProvide,
                                match_limit: None,
                                client_order_id: client_oid,
                                use_only_deposited_funds: false,
                                last_valid_slot: None,
                                last_valid_unix_timestamp_in_seconds: None,
                                fail_silently_on_insufficient_funds: false,
                            },
                        )?
                    }
                }
                _ => reject!("unsupported time in force"),
            };
            let mut ixs = vec![];
            // create market seat if we aren't already seated
            if !market_info.seated.load(Ordering::Relaxed) {
                let ix = sdk_client
                    .get_maker_setup_instructions_for_market(&market_key)
                    .await?;
                ixs.extend(ix);
            }
            ixs.push(ix);
            let prio_fee = market_info.prioritization_fee.load(Ordering::Relaxed);
            let sig = utils::sign_send_instructions_with_prioritization_fee(
                sdk_client,
                ixs,
                (prio_fee * 110) / 100,
            )
            .await?;
            debug!("txn submitted: {:?}", sig);
            {
                let mut open_cl_oids = ctx.open_cl_oids.lock().unwrap();
                open_cl_oids.insert(client_oid, None);
            }
        }
        ExternalCptyProtocol::Cancel(c) => {
            // find the cloid and osn corresponding to the architect orderid
            if let Some((cl_oid, Some(osn))) = {
                let ctx_cloids = ctx.cloids.lock().unwrap();
                let cloid = ctx_cloids.iter().find_map(|(cl_oid, aoid)| {
                    if *aoid == c.order_id {
                        Some(*cl_oid)
                    } else {
                        None
                    }
                });
                if let Some(cloid) = cloid {
                    let ctx_open_cl_oids = ctx.open_cl_oids.lock().unwrap();
                    if let Some(osn) = ctx_open_cl_oids.get(&cloid).copied() {
                        Some((cloid, osn))
                    } else {
                        None
                    }
                } else {
                    None
                }
            } {
                // find the correspondent market
                let mut res = None;
                for (_, market_info) in &ctx.market_info {
                    let oo = market_info.open_oids.lock().unwrap();
                    if let Some((_, pit)) = oo.get(&osn) {
                        res = Some((market_info, *pit));
                        break;
                    }
                }
                if let Some((market_info, pit)) = res {
                    let ix = sdk_client.get_cancel_ids_ix(
                        &market_info.pubkey,
                        vec![FIFOOrderId::new_from_untyped(pit, osn)],
                    )?;
                    let prio_fee = market_info.prioritization_fee.load(Ordering::Relaxed);
                    let sig = utils::sign_send_instructions_with_prioritization_fee(
                        sdk_client,
                        vec![ix],
                        (prio_fee * 110) / 100,
                    )
                    .await?;
                    debug!("ixs submitted: {:?}", sig);
                } else {
                    error!(
                        "unknown market for order: {}={} #{}",
                        c.order_id, cl_oid, osn
                    );
                }
            } else {
                error!("unknown order id or osn for: {}", c.order_id);
            }
        }
        ExternalCptyProtocol::GetBookSnapshot { id, market } => {
            let market_info = match ctx.market_info.get(&market) {
                Some(entry) => entry,
                None => {
                    let reply = ExternalCptyProtocol::BookSnapshot {
                        id,
                        result: None,
                        error: Some("market not found".to_string()),
                    };
                    let reply = serde_json::to_string(&reply)?;
                    ws.send(Message::Text(reply)).await?;
                    return Ok(());
                }
            };
            let reply = {
                let snap = market_info.book_snapshot.lock().unwrap();
                if let Some(snap) = &*snap {
                    ExternalCptyProtocol::BookSnapshot {
                        id,
                        result: Some(snap.clone()),
                        error: None,
                    }
                } else {
                    ExternalCptyProtocol::BookSnapshot {
                        id,
                        result: None,
                        error: Some("no book snapshot yet".to_string()),
                    }
                }
            };
            let reply = serde_json::to_string(&reply)?;
            ws.send(Message::Text(reply)).await?;
        }
        ExternalCptyProtocol::Symbology(..)
        | ExternalCptyProtocol::OpenOrders { .. }
        | ExternalCptyProtocol::Balances { .. }
        | ExternalCptyProtocol::Reject(..)
        | ExternalCptyProtocol::Ack(..)
        | ExternalCptyProtocol::Fill(..)
        | ExternalCptyProtocol::Out(..)
        | ExternalCptyProtocol::BookSnapshot { .. } => {
            // not expecting the recv this
        }
    }
    Ok(())
}

async fn poll_markets_task(
    sdk_client: Arc<SDKClient>,
    rpc_limiter: Arc<RateLimiter<NotKeyed, InMemoryState, DefaultClock>>,
    ctx: &ConnectionCtx,
    interval: chrono::Duration,
    trader: Pubkey,
    prioritization_fee_percentile: f64,
) -> Result<()> {
    let mut min_interval = tokio::time::interval(interval.to_std()?);
    min_interval.set_missed_tick_behavior(MissedTickBehavior::Skip);
    loop {
        if let Err(e) = poll_market::poll_market(
            sdk_client.clone(),
            &rpc_limiter,
            ctx,
            trader,
            prioritization_fee_percentile,
        )
        .await
        {
            error!("poll_balances_and_open_orders: {:?}", e);
        }
        min_interval.tick().await;
    }
}

struct SlotCursor {
    ts: i64,
    seen: FxHashSet<Signature>,
}

async fn poll_transactions_task(
    sdk_client: Arc<SDKClient>,
    rpc_limiter: Arc<RateLimiter<NotKeyed, InMemoryState, DefaultClock>>,
    ctx: &ConnectionCtx,
    bcast_tx: &mut broadcast::Sender<ExternalCptyProtocol>,
    venue: VenueId,
    address: Pubkey,
    trader: Pubkey,
) -> Result<()> {
    let mut until = None;
    let mut cursor: Option<SlotCursor> = None;
    let mut min_interval = tokio::time::interval(std::time::Duration::from_secs(1));
    min_interval.set_missed_tick_behavior(MissedTickBehavior::Skip);
    loop {
        trace!("polling transactions until {until:?}");
        if let Some(next_until) = poll_transactions(
            &sdk_client,
            &rpc_limiter,
            ctx,
            bcast_tx,
            venue,
            address,
            trader,
            until,
            &mut cursor,
        )
        .await?
        {
            until = Some(next_until);
        }
        min_interval.tick().await;
    }
}

async fn poll_transactions(
    sdk_client: &SDKClient,
    rpc_limiter: &RateLimiter<NotKeyed, InMemoryState, DefaultClock>,
    ctx: &ConnectionCtx,
    bcast_tx: &mut broadcast::Sender<ExternalCptyProtocol>,
    venue: VenueId,
    address: Pubkey, // market pubkey
    trader: Pubkey,
    until: Option<Signature>,
    cursor: &mut Option<SlotCursor>,
) -> Result<Option<Signature>> {
    let mut market_name_and_info = None;
    for (market_name, mi) in &ctx.market_info {
        if mi.pubkey == address {
            market_name_and_info = Some((market_name, mi));
            break;
        }
    }
    let (market_name, market_info) = if let Some((name, info)) = market_name_and_info {
        (name, info)
    } else {
        error!("market {address:?} not found for transaction polling");
        return Ok(until);
    };
    rpc_limiter.until_ready().await;
    let mut txns = sdk_client
        .client
        .get_signatures_for_address_with_config(
            &address,
            GetConfirmedSignaturesForAddress2Config {
                before: None,
                until,
                limit: None,
                commitment: Some(CommitmentConfig::confirmed()),
            },
        )
        .await?;
    txns.reverse();
    // ignore the first pull, and just set the cursor
    if cursor.is_none() {
        if let Some(last_txn) = txns.last() {
            if let Some(ts) = last_txn.block_time {
                *cursor = Some(SlotCursor { ts, seen: FxHashSet::default() });
                let sig: Signature = last_txn.signature.parse()?;
                return Ok(Some(sig));
            }
        }
        warn!("first txn poll wasn't the expected shape, trying again next poll");
        return Ok(None);
    }
    let cursor = cursor.as_mut().unwrap();
    let mut filtered = vec![];
    for txn in &txns {
        if let Some(ts) = txn.block_time {
            if ts < cursor.ts {
                continue; // already seen this slot ts, ignoring
            }
            let sig = txn.signature.parse()?;
            if ts == cursor.ts {
                if cursor.seen.contains(&sig) {
                    continue; // already seen this signature, ignoring
                }
            } else if ts > cursor.ts {
                cursor.ts = ts;
                cursor.seen.clear();
            }
            filtered.push(txn);
        }
    }
    let mut next_until = until;
    if filtered.len() > 0 {
        let last_txn = filtered.last().unwrap();
        let last_sig: Signature = last_txn.signature.parse()?;
        next_until = Some(last_sig);
        // parse transactions
        let from_t = Utc.timestamp_opt(cursor.ts, 0).single();
        let to_t = last_txn.block_time.and_then(|t| Utc.timestamp_opt(t, 0).single());
        let now = Utc::now();
        if let (Some(from_t), Some(to_t)) = (from_t, to_t) {
            info!(
                "processing {} transactions from {:?} to {:?} [delta to now: {}s]",
                filtered.len(),
                from_t,
                to_t,
                (now - to_t).num_seconds()
            );
        } else {
            warn!("processing {} transactions (bad timestamps)", filtered.len());
        }
        for txn in &filtered {
            let sig: Signature = txn.signature.parse()?;
            trace!("parsing signature: {:?}", sig);
            rpc_limiter.until_ready().await;
            if let Some(events) = sdk_client.parse_events_from_transaction(&sig).await {
                for event in &events {
                    match event.details {
                        MarketEventDetails::Fill(f) => {
                            if f.maker == trader || f.taker == trader {
                                let oo = market_info.open_oids.lock().unwrap();
                                if let Some((cl_oid, _)) =
                                    oo.get(&f.order_sequence_number)
                                {
                                    let oid = if let Some(oid) = {
                                        let ctx_cloids = ctx.cloids.lock().unwrap();
                                        ctx_cloids.get(cl_oid).copied()
                                    } {
                                        oid
                                    } else {
                                        error!("unknown cl_oid: {}", cl_oid);
                                        continue;
                                    };
                                    let mut fill_discriminator = BytesMut::new();
                                    fill_discriminator.put_u128(oid.seqid.as_u128());
                                    fill_discriminator.put_u64(oid.seqno);
                                    fill_discriminator.put_u64(event.sequence_number);
                                    let fill_id = FillId::from_id(
                                        venue,
                                        fill_discriminator.as_ref(),
                                    );
                                    let quantity = Decimal::from_i128_with_scale(
                                        market_info
                                            .metadata
                                            .base_lots_to_base_atoms(f.base_lots_filled)
                                            as i128,
                                        market_info.base.decimals as u32,
                                    )
                                    .normalize();
                                    let f_price = market_info
                                        .metadata
                                        .ticks_to_float_price(f.price_in_ticks);
                                    let price = if let Some(price) = Decimal::from_f64(
                                        market_info
                                            .metadata
                                            .ticks_to_float_price(f.price_in_ticks),
                                    ) {
                                        price
                                    } else {
                                        error!("unrepresentable price: {}", f_price);
                                        continue;
                                    };
                                    let dir = if f.side_filled == Side::Bid {
                                        Dir::Buy
                                    } else {
                                        Dir::Sell
                                    };
                                    let trade_time = Utc
                                        .timestamp_opt(event.timestamp, 0)
                                        .single()
                                        .unwrap_or_else(|| {
                                            warn!("fill event has bad timestamp, using system time now: {}", event.timestamp);
                                            Utc::now()
                                        });
                                    let fill = ExternalCptyProtocol::Fill(ExternalFill {
                                        kind: FillKind::Normal,
                                        fill_id,
                                        order_id: Some(oid),
                                        market: market_name.clone(),
                                        quantity,
                                        price,
                                        dir,
                                        is_maker: Some(f.maker == trader),
                                        trade_time,
                                    });
                                    bcast_tx
                                        .send(fill)
                                        .map_err(|_| anyhow!("broadcast closed"))?;
                                } else {
                                    error!("fill before ack: {f:?}",);
                                }
                            }
                        }
                        MarketEventDetails::Place(p) => {
                            let mut ocos = ctx.open_cl_oids.lock().unwrap();
                            if let Some(osn) = ocos.get_mut(&p.client_order_id) {
                                *osn = Some(p.order_sequence_number);
                                let mut oo = market_info.open_oids.lock().unwrap();
                                oo.insert(
                                    p.order_sequence_number,
                                    (p.client_order_id, p.price_in_ticks),
                                );
                                let oid = if let Some(oid) = {
                                    let ctx_cloids = ctx.cloids.lock().unwrap();
                                    ctx_cloids.get(&p.client_order_id).copied()
                                } {
                                    oid
                                } else {
                                    error!("unknown cl_oid: {}", p.client_order_id);
                                    continue;
                                };
                                info!(
                                    "architect order {} acked with osn #{}",
                                    oid, p.order_sequence_number
                                );
                                bcast_tx
                                    .send(ExternalCptyProtocol::Ack(Ack {
                                        order_id: oid,
                                    }))
                                    .map_err(|_| anyhow!("broadcast closed"))?;
                            } else if p.maker == trader {
                                debug!("trader placed: {p:?}");
                                warn!(
                                    "trader order {} acked but not known to Architect",
                                    p.client_order_id
                                );
                            }
                        }
                        MarketEventDetails::Evict(e) => {
                            let mut oo = market_info.open_oids.lock().unwrap();
                            if let Some((cl_oid, _)) =
                                oo.get(&e.order_sequence_number).cloned()
                            {
                                let mut ocos = ctx.open_cl_oids.lock().unwrap();
                                ocos.remove(&cl_oid);
                                oo.remove(&e.order_sequence_number);
                                let oid = if let Some(oid) = {
                                    let ctx_cloids = ctx.cloids.lock().unwrap();
                                    ctx_cloids.get(&cl_oid).copied()
                                } {
                                    oid
                                } else {
                                    error!("unknown cl_oid: {}", cl_oid);
                                    continue;
                                };
                                info!("architect order {} evicted out", oid);
                                bcast_tx
                                    .send(ExternalCptyProtocol::Out(Out {
                                        order_id: oid,
                                    }))
                                    .map_err(|_| anyhow!("broadcast closed"))?;
                            }
                        }
                        MarketEventDetails::Reduce(r) => {
                            let mut oo = market_info.open_oids.lock().unwrap();
                            if let Some((cl_oid, _)) =
                                oo.get(&r.order_sequence_number).cloned()
                            {
                                if r.base_lots_remaining == 0 || r.is_full_cancel {
                                    let mut ocos = ctx.open_cl_oids.lock().unwrap();
                                    ocos.remove(&cl_oid);
                                    oo.remove(&r.order_sequence_number);
                                    let oid = if let Some(oid) = {
                                        let ctx_cloids = ctx.cloids.lock().unwrap();
                                        ctx_cloids.get(&cl_oid).copied()
                                    } {
                                        oid
                                    } else {
                                        error!("unknown cl_oid: {}", cl_oid);
                                        continue;
                                    };
                                    info!("architect order {} reduced out", oid);
                                    bcast_tx
                                        .send(ExternalCptyProtocol::Out(Out {
                                            order_id: oid,
                                        }))
                                        .map_err(|_| anyhow!("broadcast closed"))?;
                                }
                            }
                        }
                        MarketEventDetails::FillSummary(..) => {}
                        MarketEventDetails::Fee(..) => {}
                        MarketEventDetails::TimeInForce(..) => {}
                    }
                }
            }
        }
    }
    Ok(next_until)
}
