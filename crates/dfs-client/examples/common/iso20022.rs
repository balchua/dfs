//! ISO 20022 pain.001 (Customer Credit Transfer Initiation) test
//! fixture generator.  Produces deterministic XML for use in examples
//! and manual verification.
//!
//! Not part of the public client API — this is an example helper.

use std::io::Write;

fn bic_from_index(idx: usize) -> String {
    let prefix = [
        "ABCD", "BCDE", "CDEF", "DEFG", "EFGH", "FGHI", "GHIJ", "HIJK",
    ][idx % 8];
    format!("{prefix}DEFF{:03}X", (idx % 999) + 1)
}

/// Generate pain.001 XML with `num_txns` credit transfers. `seed`
/// makes output deterministic — same inputs produce identical XML.
///
/// XML is written directly to `dest` via `Write`, so the caller can
/// stream to a file, a socket, etc. without buffering the entire
/// payload in memory.
pub fn generate_to<W: Write>(dest: &mut W, message_id: &str, num_txns: u32, seed: u64) {
    let mut state = seed;

    writeln!(dest, r#"<?xml version="1.0" encoding="UTF-8"?>"#).unwrap();
    writeln!(
        dest,
        r#"<Document xmlns="urn:iso:std:iso:20022:tech:xsd:pain.001.001.09">"#
    )
    .unwrap();
    writeln!(dest, r#"  <CstmrCdtTrfInitn>"#).unwrap();
    writeln!(dest, r#"    <GrpHdr>"#).unwrap();
    writeln!(dest, r#"      <MsgId>MSG-{message_id}</MsgId>"#).unwrap();
    writeln!(
        dest,
        r#"      <CreDtTm>2026-04-25T04:00:00+02:00</CreDtTm>"#
    )
    .unwrap();
    writeln!(dest, r#"      <NbOfTxs>{num_txns}</NbOfTxs>"#).unwrap();

    let mut ctrl_sum: f64 = 0.0;
    for _ in 0..num_txns {
        state = u64::wrapping_mul(state, 6364136223846793005).wrapping_add(1442695040888963407);
        let raw = (state % 9_999_999) as f64 / 100.0 + 0.01;
        let amt = (raw * 100.0).round() / 100.0;
        ctrl_sum += amt;
    }

    writeln!(dest, r#"      <CtrlSum>{ctrl_sum:.2}</CtrlSum>"#).unwrap();
    writeln!(dest, r#"      <InitgPty><Nm>PAYEE CORP AG</Nm></InitgPty>"#).unwrap();
    writeln!(dest, r#"    </GrpHdr>"#).unwrap();
    writeln!(dest, r#"    <PmtInf>"#).unwrap();
    writeln!(dest, r#"      <PmtInfId>PAY-{message_id}</PmtInfId>"#).unwrap();
    writeln!(dest, r#"      <PmtMtd>TRF</PmtMtd>"#).unwrap();
    writeln!(dest, r#"      <BtchBookg>true</BtchBookg>"#).unwrap();
    writeln!(dest, r#"      <NbOfTxs>{num_txns}</NbOfTxs>"#).unwrap();
    writeln!(dest, r#"      <CtrlSum>{ctrl_sum:.2}</CtrlSum>"#).unwrap();
    writeln!(dest, r#"      <PmtTpInf><InstrPrty>HIGH</InstrPrty><SvcLvl><Cd>SEPA</Cd></SvcLvl><CtgyPurp><Cd>SALA</Cd></CtgyPurp></PmtTpInf>"#).unwrap();
    writeln!(dest, r#"      <ReqdExctnDt>2027-04-26</ReqdExctnDt>"#).unwrap();
    writeln!(
        dest,
        r#"      <Dbtr><Nm>DEBTOR CORE HOLDINGS INC</Nm></Dbtr>"#
    )
    .unwrap();
    writeln!(
        dest,
        r#"      <DbtrAcct><Id><IBAN>DE89370400440532013000</IBAN></Id><Ccy>EUR</Ccy></DbtrAcct>"#
    )
    .unwrap();
    writeln!(
        dest,
        r#"      <DbtrAgt><FinInstnId><BICFI>DEUTDEFFXXX</BICFI></FinInstnId></DbtrAgt>"#
    )
    .unwrap();
    writeln!(dest, r#"      <ChrgBr>SLEV</ChrgBr>"#).unwrap();

    state = seed;
    for idx in 0..num_txns as usize {
        state = u64::wrapping_mul(state, 6364136223846793005).wrapping_add(1442695040888963407);
        let raw = (state % 9_999_999) as f64 / 100.0 + 0.01;
        let amt = (raw * 100.0).round() / 100.0;

        let txn = idx + 1;
        let bic = bic_from_index(idx);
        let cred_no = (idx % 10_000) + 1;
        writeln!(dest, r#"      <CdtTrfTxInf>"#).unwrap();
        writeln!(dest, r#"        <PmtId>"#).unwrap();
        writeln!(dest, r#"          <InstrId>TXN-{txn:06}</InstrId>"#).unwrap();
        writeln!(
            dest,
            r#"          <EndToEndId>E2E-{message_id}-{txn:06}</EndToEndId>"#
        )
        .unwrap();
        writeln!(dest, r#"        </PmtId>"#).unwrap();
        writeln!(
            dest,
            r#"        <Amt><InstdAmt Ccy="EUR">{amt:.2}</InstdAmt></Amt>"#
        )
        .unwrap();
        writeln!(dest, r#"        <CdtrAgt>"#).unwrap();
        writeln!(
            dest,
            r#"          <FinInstnId><BICFI>{bic}</BICFI></FinInstnId>"#
        )
        .unwrap();
        writeln!(
            dest,
            r#"          <FinInstnId><Nm>BANK {txn:06}</Nm></FinInstnId>"#
        )
        .unwrap();
        writeln!(dest, r#"        </CdtrAgt>"#).unwrap();
        writeln!(dest, r#"        <Cdtr>"#).unwrap();
        writeln!(dest, r#"          <Nm>CREDITOR {cred_no:06}</Nm>"#).unwrap();
        writeln!(dest, r#"          <PstlAdr><StrtNm>{txn} High Street</StrtNm><BldgNb>{cred_no}</BldgNb><PstCd>12345</PstCd><TwnNm>Berlin</TwnNm><Ctry>DE</Ctry></PstlAdr>"#).unwrap();
        writeln!(dest, r#"          <Id><OrgId><Othr><Id>CRED-{txn:06}</Id><SchmeNm><Cd>TXID</Cd></SchmeNm></Othr></OrgId></Id>"#).unwrap();
        writeln!(dest, r#"        </Cdtr>"#).unwrap();
        writeln!(dest, r#"        <CdtrAcct><Id><IBAN>DE12345678901234567890</IBAN></Id><Ccy>EUR</Ccy></CdtrAcct>"#).unwrap();
        writeln!(dest, r#"        <Purp><Cd>CBFT</Cd></Purp>"#).unwrap();
        writeln!(dest, r#"        <RmtInf>"#).unwrap();
        writeln!(
            dest,
            r#"          <Ustrd>Payment INV-{txn:08} — Ref: {message_id}</Ustrd>"#
        )
        .unwrap();
        writeln!(dest, r#"          <Strd><CdtrRefInf>"#).unwrap();
        writeln!(
            dest,
            r#"            <Tp><CdOrPrtry><Cd>SCOR</Cd></CdOrPrtry></Tp>"#
        )
        .unwrap();
        writeln!(dest, r#"            <Ref>REF-{message_id}-{txn:06}</Ref>"#).unwrap();
        writeln!(dest, r#"          </CdtrRefInf></Strd>"#).unwrap();
        writeln!(dest, r#"        </RmtInf>"#).unwrap();
        writeln!(dest, r#"        <UltmtDbtr><Nm>ULTIMATE DEBTOR {cred_no:06}</Nm><Id><OrgId><Othr><Id>ULT-{txn:06}</Id></Othr></OrgId></Id></UltmtDbtr>"#).unwrap();
        writeln!(dest, r#"      </CdtTrfTxInf>"#).unwrap();
    }

    writeln!(dest, r#"    </PmtInf>"#).unwrap();
    writeln!(dest, r#"  </CstmrCdtTrfInitn>"#).unwrap();
    writeln!(dest, r#"</Document>"#).unwrap();
}
