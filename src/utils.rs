use anyhow::{anyhow, bail, Result};
use log::{error, info};
use phoenix_sdk::sdk_client::SDKClient;
use phoenix_sdk_core::ata_utils::{
    create_associated_token_account, get_associated_token_address,
};
use solana_sdk::{
    account::Account,
    compute_budget::ComputeBudgetInstruction,
    instruction::Instruction,
    program_pack::Pack,
    pubkey::Pubkey,
    signature::{read_keypair_file, Keypair, Signature},
    signer::Signer,
    transaction::Transaction,
};
use spl_token::state::Mint;

pub fn get_payer_keypair() -> solana_sdk::signer::keypair::Keypair {
    match std::env::var("PAYER").is_ok() {
        true => Keypair::from_base58_string(
            &std::env::var("PAYER").expect("$PAYER is not set")[..],
        ),
        false => read_keypair_file(&*shellexpand::tilde("~/.config/solana/id.json"))
            .map_err(|e| anyhow!(e.to_string()))
            .unwrap(),
    }
}

pub async fn sign_send_instructions_with_prioritization_fee(
    sdk_client: &SDKClient,
    instructions: Vec<Instruction>,
    prioritization_fee: u64,
) -> Result<Signature> {
    let mut ixs = vec![];
    let recent_blockhash = sdk_client.client.get_latest_blockhash().await?;
    let txn = Transaction::new_signed_with_payer(
        &instructions,
        Some(&sdk_client.client.payer.pubkey()),
        &[&sdk_client.client.payer],
        recent_blockhash,
    );
    let res = sdk_client.client.simulate_transaction(&txn).await?;
    if let Some(err) = res.value.err {
        error!("simulating transaction: {err:?}");
        bail!("simulating transaction: {err:?}");
    }
    let cu = res.value.units_consumed.ok_or_else(|| anyhow!("simulation error"))? as u32;
    ixs.push(ComputeBudgetInstruction::set_compute_unit_limit(cu + (cu / 10)));
    ixs.push(ComputeBudgetInstruction::set_compute_unit_price(prioritization_fee));
    ixs.extend(instructions);
    let sig = sdk_client.client.sign_send_instructions(ixs, vec![]).await?;
    // TODO: figure out how to catch a txfail or drop (probably wait 3s)
    Ok(sig)
}

pub async fn create_airdrop_spl_ixs(
    sdk_client: &SDKClient,
    market_pubkey: &Pubkey,
    recipient_pubkey: &Pubkey,
) -> Option<Vec<Instruction>> {
    // Get base and quote mints from market metadata
    let market_metadata = sdk_client.get_market_metadata(market_pubkey).await.ok()?;
    let base_mint = market_metadata.base_mint;
    let quote_mint = market_metadata.quote_mint;
    let mint_accounts = sdk_client
        .client
        .get_multiple_accounts(&[base_mint, quote_mint])
        .await
        .unwrap()
        .into_iter()
        .flatten()
        .collect::<Vec<Account>>();
    let base_mint_account = Mint::unpack(&mint_accounts[0].data).unwrap();
    let quote_mint_account = Mint::unpack(&mint_accounts[1].data).unwrap();
    let base_mint_authority = base_mint_account.mint_authority.unwrap();
    let quote_mint_authority = quote_mint_account.mint_authority.unwrap();
    let mint_authority_accounts = sdk_client
        .client
        .get_multiple_accounts(&[base_mint_authority, quote_mint_authority])
        .await
        .unwrap()
        .into_iter()
        .flatten()
        .collect::<Vec<Account>>();
    // If either the base or quote mint authority accounts (PDAs) are not owned by the devnet token faucet program, abort minting
    if mint_authority_accounts[0].owner != generic_token_faucet::id()
        || mint_authority_accounts[1].owner != generic_token_faucet::id()
    {
        return None;
    }
    // Get or create the ATA for the recipient. If doesn't exist, create token account
    let mut instructions = vec![];
    let recipient_ata_base = get_associated_token_address(recipient_pubkey, &base_mint);
    let recipient_ata_quote = get_associated_token_address(recipient_pubkey, &quote_mint);
    let recipient_ata_accounts = sdk_client
        .client
        .get_multiple_accounts(&[recipient_ata_base, recipient_ata_quote])
        .await
        .unwrap();
    if recipient_ata_accounts[0].is_none() {
        info!("error retrieving base ATA; creating base ATA...");
        instructions.push(create_associated_token_account(
            &sdk_client.client.payer.pubkey(),
            recipient_pubkey,
            &base_mint,
            &spl_token::id(),
        ))
    };
    if recipient_ata_accounts[1].is_none() {
        info!("error retrieving quote ATA; creating quote ATA...");
        instructions.push(create_associated_token_account(
            &sdk_client.client.payer.pubkey(),
            recipient_pubkey,
            &quote_mint,
            &spl_token::id(),
        ))
    };
    // Finally, mint the base and quote tokens to the recipient. The recipient's ATAs will be automatically derived.
    instructions.push(generic_token_faucet::airdrop_spl_with_mint_pdas_ix(
        &generic_token_faucet::id(),
        &base_mint,
        &base_mint_authority,
        recipient_pubkey,
        (5000.0 * 1e9) as u64,
    ));
    instructions.push(generic_token_faucet::airdrop_spl_with_mint_pdas_ix(
        &generic_token_faucet::id(),
        &quote_mint,
        &quote_mint_authority,
        recipient_pubkey,
        (500000.0 * 1e6) as u64,
    ));
    Some(instructions)
}
