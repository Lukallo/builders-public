// Minimal Parquet writer (one row group, one PLAIN uncompressed data page per
// column) and the two output tables.
use crate::par::par_map;
use std::io::Write;
use std::path::Path;

pub struct UserRow {
    pub person: u128,
    pub day: i32,
    pub sessions: i64,
    pub events: i64,
    pub revenue: i64,
    pub top: Option<u16>,
}
pub struct CampRow {
    pub id: Vec<u8>,
    pub channel: Vec<u8>,
    pub day: i32,
    pub sessions: i64,
    pub purchasing: i64,
    pub revenue: i64,
    pub assisted: i64,
}

// ---------- thrift compact protocol ----------
struct T {
    b: Vec<u8>,
    last: Vec<i16>,
}
const CT_I32: u8 = 5;
const CT_I64: u8 = 6;
const CT_BIN: u8 = 8;
const CT_LIST: u8 = 9;
const CT_STRUCT: u8 = 12;
impl T {
    fn new() -> T {
        T { b: vec![], last: vec![0] }
    }
    fn varint(&mut self, mut v: u64) {
        while v >= 0x80 {
            self.b.push((v as u8) | 0x80);
            v >>= 7;
        }
        self.b.push(v as u8);
    }
    fn zz(&mut self, v: i64) {
        self.varint(((v << 1) ^ (v >> 63)) as u64);
    }
    fn fh(&mut self, id: i16, ty: u8) {
        let last = *self.last.last().unwrap();
        let d = id - last;
        if d > 0 && d <= 15 {
            self.b.push(((d as u8) << 4) | ty);
        } else {
            self.b.push(ty);
            self.zz(id as i64);
        }
        *self.last.last_mut().unwrap() = id;
    }
    fn i32(&mut self, id: i16, v: i32) {
        self.fh(id, CT_I32);
        self.zz(v as i64);
    }
    fn i64(&mut self, id: i16, v: i64) {
        self.fh(id, CT_I64);
        self.zz(v);
    }
    fn bin(&mut self, id: i16, v: &[u8]) {
        self.fh(id, CT_BIN);
        self.varint(v.len() as u64);
        self.b.extend_from_slice(v);
    }
    fn begin(&mut self, id: i16) {
        self.fh(id, CT_STRUCT);
        self.last.push(0);
    }
    fn begin_elem(&mut self) {
        self.last.push(0);
    }
    fn end(&mut self) {
        self.b.push(0);
        self.last.pop();
    }
    fn list(&mut self, id: i16, ety: u8, n: usize) {
        self.fh(id, CT_LIST);
        if n < 15 {
            self.b.push(((n as u8) << 4) | ety);
        } else {
            self.b.push(0xF0 | ety);
            self.varint(n as u64);
        }
    }
}

// physical types
const INT32: i32 = 1;
const INT64: i32 = 2;
const BYTE_ARRAY: i32 = 6;

#[derive(Clone, Copy, PartialEq)]
enum Kind {
    Str,
    Date,
    Int,
}

pub enum Col {
    Bytes(Vec<u8>, usize), // PLAIN encoded values, count
    I32(Vec<u8>, usize),
    I64(Vec<u8>, usize),
}

struct ColSpec {
    name: &'static str,
    kind: Kind,
    optional: bool,
}

fn write_file(path: &Path, specs: &[ColSpec], cols: Vec<(Col, Option<Vec<bool>>)>, nrows: usize) {
    let mut f = Vec::<u8>::with_capacity(1 << 20);
    f.extend_from_slice(b"PAR1");
    struct Meta {
        offset: i64,
        size: i64,
        ptype: i32,
        nvals: i64,
    }
    let mut metas = vec![];
    for (spec, (col, defs)) in specs.iter().zip(cols.into_iter()) {
        let (ptype, data) = match col {
            Col::Bytes(d, _) => (BYTE_ARRAY, d),
            Col::I32(d, _) => (INT32, d),
            Col::I64(d, _) => (INT64, d),
        };
        let mut page = vec![];
        if spec.optional {
            // RLE/bit-packed hybrid, bit width 1, bit-packed runs
            let defs = defs.unwrap();
            let mut levels = vec![];
            let groups = nrows.div_ceil(8);
            if groups > 0 {
                let mut hdr = T::new();
                hdr.varint(((groups as u64) << 1) | 1);
                levels.extend_from_slice(&hdr.b);
                for g in 0..groups {
                    let mut byte = 0u8;
                    for k in 0..8 {
                        let i = g * 8 + k;
                        if i < nrows && defs[i] {
                            byte |= 1 << k;
                        }
                    }
                    levels.push(byte);
                }
            }
            page.extend_from_slice(&(levels.len() as u32).to_le_bytes());
            page.extend_from_slice(&levels);
        }
        page.extend_from_slice(&data);
        let mut h = T::new();
        h.i32(1, 0); // DATA_PAGE
        h.i32(2, page.len() as i32);
        h.i32(3, page.len() as i32);
        h.begin(5);
        h.i32(1, nrows as i32);
        h.i32(2, 0); // PLAIN
        h.i32(3, 3); // RLE
        h.i32(4, 3);
        h.end();
        h.b.push(0); // end PageHeader
        let offset = f.len() as i64;
        f.extend_from_slice(&h.b);
        f.extend_from_slice(&page);
        metas.push(Meta { offset, size: (h.b.len() + page.len()) as i64, ptype, nvals: nrows as i64 });
    }
    // FileMetaData
    let mut m = T::new();
    m.i32(1, 1);
    m.list(2, CT_STRUCT, specs.len() + 1);
    m.begin_elem();
    m.bin(4, b"schema");
    m.i32(5, specs.len() as i32);
    m.end();
    for (spec, meta) in specs.iter().zip(metas.iter()) {
        m.begin_elem();
        m.i32(1, meta.ptype);
        m.i32(3, if spec.optional { 1 } else { 0 });
        m.bin(4, spec.name.as_bytes());
        match spec.kind {
            Kind::Str => {
                m.i32(6, 0); // UTF8
                m.begin(10);
                m.begin(1); // STRING
                m.end();
                m.end();
            }
            Kind::Date => {
                m.i32(6, 6); // DATE
                m.begin(10);
                m.begin(6); // DATE
                m.end();
                m.end();
            }
            Kind::Int => {}
        }
        m.end();
    }
    m.i64(3, nrows as i64);
    m.list(4, CT_STRUCT, 1);
    m.begin_elem();
    m.list(1, CT_STRUCT, specs.len());
    let total: i64 = metas.iter().map(|x| x.size).sum();
    for (spec, meta) in specs.iter().zip(metas.iter()) {
        m.begin_elem();
        m.i64(2, meta.offset);
        m.begin(3);
        m.i32(1, meta.ptype);
        m.list(2, CT_I32, 2);
        m.zz(0); // PLAIN
        m.zz(3); // RLE
        m.list(3, CT_BIN, 1);
        m.varint(spec.name.len() as u64);
        m.b.extend_from_slice(spec.name.as_bytes());
        m.i32(4, 0); // UNCOMPRESSED
        m.i64(5, meta.nvals);
        m.i64(6, meta.size);
        m.i64(7, meta.size);
        m.i64(9, meta.offset);
        m.end();
        m.end();
    }
    m.i64(2, total);
    m.i64(3, nrows as i64);
    m.end();
    m.end();
    f.extend_from_slice(&m.b);
    f.extend_from_slice(&(m.b.len() as u32).to_le_bytes());
    f.extend_from_slice(b"PAR1");
    let mut file = std::fs::File::create(path).unwrap();
    file.write_all(&f).unwrap();
}

fn put_bytes(v: &mut Vec<u8>, s: &[u8]) {
    v.extend_from_slice(&(s.len() as u32).to_le_bytes());
    v.extend_from_slice(s);
}

fn write_user(path: &Path, rows: &[UserRow], cats: &[Vec<u8>]) {
    let n = rows.len();
    let mut ids = Vec::with_capacity(n * 14);
    let mut days = Vec::with_capacity(n * 4);
    let mut ses = Vec::with_capacity(n * 8);
    let mut evs = Vec::with_capacity(n * 8);
    let mut rev = Vec::with_capacity(n * 8);
    let mut top = Vec::with_capacity(n * 8);
    let mut defs = Vec::with_capacity(n);
    for r in rows {
        put_bytes(&mut ids, &crate::key_bytes(r.person));
        days.extend_from_slice(&r.day.to_le_bytes());
        ses.extend_from_slice(&r.sessions.to_le_bytes());
        evs.extend_from_slice(&r.events.to_le_bytes());
        rev.extend_from_slice(&r.revenue.to_le_bytes());
        if let Some(c) = r.top {
            put_bytes(&mut top, &cats[c as usize]);
        }
        defs.push(r.top.is_some());
    }
    let specs = [
        ColSpec { name: "user_id", kind: Kind::Str, optional: false },
        ColSpec { name: "day", kind: Kind::Date, optional: false },
        ColSpec { name: "sessions", kind: Kind::Int, optional: false },
        ColSpec { name: "events", kind: Kind::Int, optional: false },
        ColSpec { name: "revenue_cents", kind: Kind::Int, optional: false },
        ColSpec { name: "top_category", kind: Kind::Str, optional: true },
    ];
    let cols = vec![
        (Col::Bytes(ids, n), None),
        (Col::I32(days, n), None),
        (Col::I64(ses, n), None),
        (Col::I64(evs, n), None),
        (Col::I64(rev, n), None),
        (Col::Bytes(top, n), Some(defs)),
    ];
    write_file(path, &specs, cols, n);
}

fn write_camp(path: &Path, rows: &[CampRow]) {
    let n = rows.len();
    let (mut ids, mut ch, mut days) = (vec![], vec![], vec![]);
    let mut ints: [Vec<u8>; 4] = Default::default();
    for r in rows {
        put_bytes(&mut ids, &r.id);
        put_bytes(&mut ch, &r.channel);
        days.extend_from_slice(&r.day.to_le_bytes());
        for (k, v) in [r.sessions, r.purchasing, r.revenue, r.assisted].iter().enumerate() {
            ints[k].extend_from_slice(&v.to_le_bytes());
        }
    }
    let specs = [
        ColSpec { name: "campaign_id", kind: Kind::Str, optional: false },
        ColSpec { name: "channel", kind: Kind::Str, optional: false },
        ColSpec { name: "day", kind: Kind::Date, optional: false },
        ColSpec { name: "sessions", kind: Kind::Int, optional: false },
        ColSpec { name: "purchasing_sessions", kind: Kind::Int, optional: false },
        ColSpec { name: "revenue_cents", kind: Kind::Int, optional: false },
        ColSpec { name: "assisted_sessions", kind: Kind::Int, optional: false },
    ];
    let [a, b, c, d] = ints;
    let cols = vec![
        (Col::Bytes(ids, n), None),
        (Col::Bytes(ch, n), None),
        (Col::I32(days, n), None),
        (Col::I64(a, n), None),
        (Col::I64(b, n), None),
        (Col::I64(c, n), None),
        (Col::I64(d, n), None),
    ];
    write_file(path, &specs, cols, n);
}

pub fn write_all(out: &Path, users: Vec<Vec<UserRow>>, camps: Vec<CampRow>, cats: &[Vec<u8>]) {
    let ud = out.join("user_daily");
    let cd = out.join("campaign_daily");
    std::fs::create_dir_all(&ud).unwrap();
    std::fs::create_dir_all(&cd).unwrap();
    // group the per-bucket row vectors into a few files of similar size
    let nfiles = 16usize.min(users.len().max(1));
    let mut groups: Vec<Vec<UserRow>> = (0..nfiles).map(|_| vec![]).collect();
    for (i, u) in users.into_iter().enumerate() {
        groups[i % nfiles].extend(u);
    }
    let camps_slot = std::sync::Mutex::new(Some(camps));
    par_map(nfiles + 1, |i| {
        if i == nfiles {
            let c = camps_slot.lock().unwrap().take().unwrap();
            write_camp(&cd.join("part-000.parquet"), &c);
        } else {
            write_user(&ud.join(format!("part-{i:03}.parquet")), &groups[i], cats);
        }
    });
}
