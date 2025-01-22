use api::{cpty::generic_external::ExternalBookSnapshot, OrderId};
use phoenix_sdk_core::sdk_client_core::MarketMetadata;
use rust_decimal::Decimal;
use solana_sdk::pubkey::Pubkey;
use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicBool, AtomicU64},
        Arc, Mutex,
    },
};

pub mod poll_market;
pub mod utils;

#[derive(Debug)]
pub struct ConnectionCtx {
    pub market_info: HashMap<String, MarketInfo>,
    pub balances: Mutex<HashMap<String, Decimal>>,
    // client_order_id => order sequence number
    pub open_cl_oids: Mutex<HashMap<u128, Option<u64>>>,
    // client_order_id => order id
    pub cloids: Mutex<HashMap<u128, OrderId>>,
}

#[derive(Debug)]
pub struct MarketInfo {
    pub pubkey: Pubkey,
    pub tick_size: Decimal,
    pub step_size: Decimal,
    // last recommended priorization fee for this market in micro_lamports
    pub prioritization_fee: Arc<AtomicU64>,
    pub metadata: MarketMetadata,
    pub base: TokenInfo,
    pub quote: TokenInfo,
    // does the trader have a seat in the market
    pub seated: Arc<AtomicBool>,
    // open order sequence numbers => (client_order_id, price_in_ticks)
    // TODO: persist these across restarts
    pub open_oids: Mutex<HashMap<u64, (u128, u64)>>,
    pub book_snapshot: Mutex<Option<ExternalBookSnapshot>>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct TokenInfo {
    pub pubkey: Pubkey,
    pub name: String,
    pub decimals: u8,
    pub atas: Vec<Pubkey>,
}
