//! ISO 20022 pain.001 (Customer Credit Transfer Initiation) test
//! fixture generator.  Produces deterministic XML for use in examples
//! and manual verification.
//!
//! Not part of the public client API — this is an example helper.

use std::io::Write;

fn bic_from_index(idx: usize) -> String {
    let prefix = ["ABCD", "BCDE", "CDEF", "DEFG", "EFGH", "FGHI", "GHIJ", "HIJK"][idx % 8];
    format!("{prefix}DEFF{:03}X", (idx % 999) + 1)
}

/// Generate pain.001 XML with `num_txns` credit transfers. `seed`
/// makes output deterministic — same inputs produce identical XML.
pub fn generate(message_id: &str, num_txns: u32, seed: u64) -> String {
    let mut state = seed;
    let amounts: Vec<f64> = (0..num_txns)
        .map(|_| {
            state = u64::wrapping_mul(state, 6364136223846793005)
                .wrapping_add(1442695040888963407);
            let raw = (state % 9_999_999) as f64 / 100.0 + 0.01;
            (raw * 100.0).round() / 100.0
        })
        .collect();

    let ctrl_sum: f64 = amounts.iter().sum();
    let mut b: Vec<u8> = Vec::with_capacity(num_txns as usize * 600);

    writeln!(b, r#"<?xml version="1.0" encoding="UTF-8"?>"#).unwrap();
    writeln!(b, r#"<Document xmlns="urn:iso:std:iso:20022:tech:xsd:pain.001.001.09">"#).unwrap();
    writeln!(b, r#"  <CstmrCdtTrfInitn>"#).unwrap();
    writeln!(b, r#"    <GrpHdr>"#).unwrap();
    writeln!(b, r#"      <MsgId>MSG-{message_id}</MsgId>"#).unwrap();
    writeln!(b, r#"      <CreDtTm>2026-04-25T04:00:00+02:00</CreDtTm>"#).unwrap();
    writeln!(b, r#"      <NbOfTxs>{num_txns}</NbOfTxs>"#).unwrap();
    writeln!(b, r#"      <CtrlSum>{ctrl_sum:.2}</CtrlSum>"#).unwrap();
    writeln!(b, r#"      <InitgPty><Nm>PAYEE CORP AG</Nm></InitgPty>"#).unwrap();
    writeln!(b, r#"    </GrpHdr>"#).unwrap();

    writeln!(b, r#"    <PmtInf>"#).unwrap();
    writeln!(b, r#"      <PmtInfId>PAY-{message_id}</PmtInfId>"#).unwrap();
    writeln!(b, r#"      <PmtMtd>TRF</PmtMtd>"#).unwrap();
    writeln!(b, r#"      <BtchBookg>true</BtchBookg>"#).unwrap();
    writeln!(b, r#"      <NbOfTxs>{num_txns}</NbOfTxs>"#).unwrap();
    writeln!(b, r#"      <CtrlSum>{ctrl_sum:.2}</CtrlSum>"#).unwrap();
    writeln!(b, r#"      <PmtTpInf><InstrPrty>HIGH</InstrPrty><SvcLvl><Cd>SEPA</Cd></SvcLvl><CtgyPurp><Cd>SALA</Cd></CtgyPurp></PmtTpInf>"#).unwrap();
    writeln!(b, r#"      <ReqdExctnDt>2027-04-26</ReqdExctnDt>"#).unwrap();
    writeln!(b, r#"      <Dbtr><Nm>DEBTOR CORE HOLDINGS INC</Nm></Dbtr>"#).unwrap();
    writeln!(b, r#"      <DbtrAcct><Id><IBAN>DE89370400440532013000</IBAN></Id><Ccy>EUR</Ccy></DbtrAcct>"#).unwrap();
    writeln!(b, r#"      <DbtrAgt><FinInstnId><BICFI>DEUTDEFFXXX</BICFI></FinInstnId></DbtrAgt>"#).unwrap();
    writeln!(b, r#"      <ChrgBr>SLEV</ChrgBr>"#).unwrap();

    for (idx, &amt) in amounts.iter().enumerate() {
        let txn = idx + 1;
        let bic = bic_from_index(idx);
        writeln!(b, r#"      <CdtTrfTxInf>"#).unwrap();
        writeln!(b, r#"        <PmtId>"#).unwrap();
        writeln!(b, r#"          <InstrId>TXN-{txn:06}</InstrId>"#).unwrap();
        writeln!(b, r#"          <EndToEndId>E2E-{message_id}-{txn:06}</EndToEndId>"#).unwrap();
        writeln!(b, r#"        </PmtId>"#).unwrap();
        writeln!(b, r#"        <Amt><InstdAmt Ccy="EUR">{amt:.2}</InstdAmt></Amt>"#).unwrap();
        writeln!(b, r#"        <CdtrAgt>"#).unwrap();
        writeln!(b, r#"          <FinInstnId><BICFI>{bic}</BICFI></FinInstnId>"#).unwrap();
        writeln!(b, r#"          <FinInstnId><Nm>BANK {txn:06}</Nm></FinInstnId>"#).unwrap();
        writeln!(b, r#"        </CdtrAgt>"#).unwrap();
        let cred_no = (idx % 10_000) + 1;
        writeln!(b, r#"        <Cdtr>"#).unwrap();
        writeln!(b, r#"          <Nm>CREDITOR {cred_no:06}</Nm>"#).unwrap();
        writeln!(b, r#"          <PstlAdr><StrtNm>{txn} High Street</StrtNm><BldgNb>{cred_no}</BldgNb><PstCd>12345</PstCd><TwnNm>Berlin</TwnNm><Ctry>DE</Ctry></PstlAdr>"#).unwrap();
        writeln!(b, r#"          <Id><OrgId><Othr><Id>CRED-{txn:06}</Id><SchmeNm><Cd>TXID</Cd></SchmeNm></Othr></OrgId></Id>"#).unwrap();
        writeln!(b, r#"        </Cdtr>"#).unwrap();
        writeln!(b, r#"        <CdtrAcct><Id><IBAN>DE12345678901234567890</IBAN></Id><Ccy>EUR</Ccy></CdtrAcct>"#).unwrap();
        writeln!(b, r#"        <Purp><Cd>CBFT</Cd></Purp>"#).unwrap();
        writeln!(b, r#"        <RmtInf>"#).unwrap();
        writeln!(b, r#"          <Ustrd>Payment INV-{txn:08} — Ref: {message_id}</Ustrd>"#).unwrap();
        writeln!(b, r#"          <Strd><CdtrRefInf>"#).unwrap();
        writeln!(b, r#"            <Tp><CdOrPrtry><Cd>SCOR</Cd></CdOrPrtry></Tp>"#).unwrap();
        writeln!(b, r#"            <Ref>REF-{message_id}-{txn:06}</Ref>"#).unwrap();
        writeln!(b, r#"          </CdtrRefInf></Strd>"#).unwrap();
        writeln!(b, r#"        </RmtInf>"#).unwrap();
        writeln!(b, r#"        <UltmtDbtr><Nm>ULTIMATE DEBTOR {cred_no:06}</Nm><Id><OrgId><Othr><Id>ULT-{txn:06}</Id></Othr></OrgId></Id></UltmtDbtr>"#).unwrap();
        writeln!(b, r#"      </CdtTrfTxInf>"#).unwrap();
    }

    writeln!(b, r#"    </PmtInf>"#).unwrap();
    writeln!(b, r#"  </CstmrCdtTrfInitn>"#).unwrap();
    writeln!(b, r#"</Document>"#).unwrap();

    String::from_utf8(b).expect("valid UTF-8")
}