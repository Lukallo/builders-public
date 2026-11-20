// Shopping Sessions pipeline: clean, link, sessionize, attribute, aggregate.
use std::collections::HashMap;
use std::hash::{BuildHasherDefault, Hasher};
use std::path::{Path, PathBuf};
use std::sync::Mutex;

mod alloc;
mod out;
#[global_allocator]
static GLOBAL: alloc::Huge = alloc::Huge;
mod par;
use par::{each_record, par_map, Source};

const SEC: i64 = 1000;
const MIN: i64 = 60 * SEC;
const HOUR: i64 = 60 * MIN;
const DAY: i64 = 24 * HOUR;
const WEEK: i64 = 7 * DAY;
const GAP: i64 = 30 * MIN;
const NB: usize = 256; // hash buckets

// ---------- hashing ----------
#[derive(Default, Clone, Copy)]
pub struct FxH(u64);
impl Hasher for FxH {
    fn finish(&self) -> u64 {
        self.0
    }
    fn write(&mut self, bytes: &[u8]) {
        for &b in bytes {
            self.0 = (self.0.rotate_left(5) ^ b as u64).wrapping_mul(0x51_7c_c1_b7_27_22_0a_95);
        }
    }
    fn write_u128(&mut self, v: u128) {
        self.0 = mix(v);
    }
    fn write_u64(&mut self, v: u64) {
        self.0 = mix(v as u128);
    }
}
pub type FxBuild = BuildHasherDefault<FxH>;
pub type FxMap<K, V> = HashMap<K, V, FxBuild>;

#[inline]
pub fn mix(v: u128) -> u64 {
    let x = (v as u64) ^ ((v >> 64) as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15);
    let m = (x as u128) * 0xD6E8_FEB8_6659_FD93u128;
    (m as u64) ^ ((m >> 64) as u64)
}
#[inline]
fn bucket(v: u128) -> usize {
    (mix(v) >> 56) as usize
}

// ---------- open-addressing u128 -> u128 map (key 0 = empty) ----------
pub struct KeyMap {
    len: usize,
    mask: usize,
    slots: Vec<[u128; 2]>,
}
impl KeyMap {
    pub fn new(n: usize) -> KeyMap {
        let cap = (n * 3 / 2 + 16).next_power_of_two();
        KeyMap { len: 0, mask: cap - 1, slots: vec![[0, 0]; cap] }
    }
    #[inline]
    fn idx(&self, k: u128) -> usize {
        (mix(k) as usize) & self.mask
    }
    pub fn insert(&mut self, k: u128, v: u128) {
        if self.len * 3 >= self.slots.len() * 2 {
            let old = std::mem::take(&mut self.slots);
            self.slots = vec![[0, 0]; old.len() * 2];
            self.mask = self.slots.len() - 1;
            self.len = 0;
            for s in old {
                if s[0] != 0 {
                    self.insert(s[0], s[1]);
                }
            }
        }
        let mut i = self.idx(k);
        loop {
            let s = &mut self.slots[i];
            if s[0] == 0 || s[0] == k {
                if s[0] == 0 {
                    self.len += 1;
                }
                *s = [k, v];
                return;
            }
            i = (i + 1) & self.mask;
        }
    }
    #[inline]
    pub fn prefetch(&self, k: u128) {
        #[cfg(target_arch = "x86_64")]
        unsafe {
            use std::arch::x86_64::{_MM_HINT_T0, _mm_prefetch};
            _mm_prefetch(self.slots.as_ptr().add(self.idx(k)) as *const i8, _MM_HINT_T0);
        }
    }
    #[inline]
    pub fn get(&self, k: u128) -> Option<u128> {
        let mut i = self.idx(k);
        loop {
            let s = self.slots[i];
            if s[0] == k {
                return Some(s[1]);
            }
            if s[0] == 0 {
                return None;
            }
            i = (i + 1) & self.mask;
        }
    }
    #[inline]
    pub fn get_or_self(&self, k: u128) -> u128 {
        let mut i = self.idx(k);
        loop {
            let s = self.slots[i];
            if s[0] == k {
                return s[1];
            }
            if s[0] == 0 {
                return k;
            }
            i = (i + 1) & self.mask;
        }
    }
}

// ---------- string keys ----------
// Strings up to 15 bytes are packed big-endian with the length in the low byte,
// so u128 order equals byte-wise string order. Longer strings are interned and
// get a key with low byte 0xFF.
static LONG: Mutex<Vec<Vec<u8>>> = Mutex::new(Vec::new());
static LONG_IDX: Mutex<Option<HashMap<Vec<u8>, u128>>> = Mutex::new(None);

#[cfg(all(target_arch = "x86_64", target_feature = "avx512bw", target_feature = "avx512vl"))]
#[inline(always)]
pub fn key(s: &[u8]) -> u128 {
    use std::arch::x86_64::*;
    let n = s.len();
    if n <= 15 {
        unsafe {
            let m: u16 = (1u16 << n) - 1;
            let v = _mm_maskz_loadu_epi8(m, s.as_ptr() as *const i8);
            // reverse bytes so byte 0 is most significant, then put len in the low byte
            let rev = _mm_set_epi8(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15);
            let r = _mm_shuffle_epi8(v, rev);
            let x: u128 = std::mem::transmute(r);
            (x >> 8 << 8) | n as u128
        }
    } else {
        long_key(s)
    }
}
#[cfg(not(all(target_arch = "x86_64", target_feature = "avx512bw", target_feature = "avx512vl")))]
#[inline]
pub fn key(s: &[u8]) -> u128 {
    if s.len() <= 15 {
        let mut buf = [0u8; 16];
        buf[..s.len()].copy_from_slice(s);
        buf[15] = s.len() as u8;
        u128::from_be_bytes(buf)
    } else {
        long_key(s)
    }
}
#[cold]
fn long_key(s: &[u8]) -> u128 {
    let mut idx = LONG_IDX.lock().unwrap();
    let m = idx.get_or_insert_with(HashMap::new);
    if let Some(&k) = m.get(s) {
        return k;
    }
    let mut l = LONG.lock().unwrap();
    let k = ((l.len() as u128) << 8) | 0xFF;
    l.push(s.to_vec());
    m.insert(s.to_vec(), k);
    k
}
pub fn key_bytes(k: u128) -> Vec<u8> {
    if k & 0xFF == 0xFF {
        LONG.lock().unwrap()[(k >> 8) as usize].clone()
    } else {
        let b = k.to_be_bytes();
        b[..(b[15] as usize)].to_vec()
    }
}
fn key_cmp(a: u128, b: u128) -> std::cmp::Ordering {
    if a & 0xFF == 0xFF || b & 0xFF == 0xFF {
        key_bytes(a).cmp(&key_bytes(b))
    } else {
        a.cmp(&b)
    }
}

// ---------- CSV ----------
/// Split a line into exactly N unquoted fields; None if malformed.
#[inline]
pub fn split<'a, const N: usize>(line: &'a [u8]) -> Option<[&'a [u8]; N]> {
    let mut out: [&[u8]; N] = [&[]; N];
    let mut n = 0;
    let mut start = 0;
    let len = line.len();
    let mut i = 0;
    loop {
        let mut quotes = 0u32;
        while i < len && line[i] != b',' {
            quotes += (line[i] == b'"') as u32;
            i += 1;
        }
        if n >= N {
            return None;
        }
        let f = &line[start..i];
        out[n] = if quotes == 0 {
            f
        } else if quotes == 2 && f.len() >= 2 && f[0] == b'"' && f[f.len() - 1] == b'"' {
            &f[1..f.len() - 1]
        } else {
            return None;
        };
        n += 1;
        if i >= len {
            break;
        }
        i += 1;
        start = i;
    }
    if n == N { Some(out) } else { None }
}

// ---------- timestamps ----------
#[inline]
fn d2(s: &[u8], i: usize) -> Option<i64> {
    let a = s[i].wrapping_sub(b'0');
    let b = s[i + 1].wrapping_sub(b'0');
    if a < 10 && b < 10 { Some((a * 10 + b) as i64) } else { None }
}
pub fn days_from_civil(y: i64, m: i64, d: i64) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = if y >= 0 { y } else { y - 399 } / 400;
    let yoe = y - era * 400;
    let mp = (m + 9) % 12;
    let doy = (153 * mp + 2) / 5 + d - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146097 + doe - 719468
}
#[inline]
fn civil(s: &[u8]) -> Option<i64> {
    // YYYY-MM-DDTHH:MM:SS
    if s[4] != b'-' || s[7] != b'-' || s[10] != b'T' || s[13] != b':' || s[16] != b':' {
        return None;
    }
    let y = d2(s, 0)? * 100 + d2(s, 2)?;
    let mo = d2(s, 5)?;
    let d = d2(s, 8)?;
    let h = d2(s, 11)?;
    let mi = d2(s, 14)?;
    let se = d2(s, 17)?;
    if !(1..=12).contains(&mo) || d < 1 || h > 23 || mi > 59 || se > 59 {
        return None;
    }
    let leap = (y % 4 == 0 && y % 100 != 0) || y % 400 == 0;
    let dim = match mo {
        2 => {
            if leap {
                29
            } else {
                28
            }
        }
        4 | 6 | 9 | 11 => 30,
        _ => 31,
    };
    if d > dim {
        return None;
    }
    Some(((days_from_civil(y, mo, d) * 24 + h) * 60 + mi) * 60_000 + se * 1000)
}
/// Parse a ts; None = bad (including F3 overflow).
#[inline]
pub fn parse_ts(s: &[u8]) -> Option<i64> {
    let n = s.len();
    if n == 0 {
        return None;
    }
    if n == 20 && s[19] == b'Z' {
        return civil(s);
    }
    if n == 25 && (s[19] == b'+' || s[19] == b'-') && s[22] == b':' {
        let base = civil(s)?;
        let oh = d2(s, 20)?;
        let om = d2(s, 23)?;
        if oh > 23 || om > 59 {
            return None;
        }
        let off = (oh * 60 + om) * MIN;
        return Some(if s[19] == b'+' { base - off } else { base + off });
    }
    // F3: digits only
    let mut v: u64 = 0;
    let mut over = false;
    for &c in s {
        let d = c.wrapping_sub(b'0');
        if d >= 10 {
            return None;
        }
        if !over {
            match v.checked_mul(10).and_then(|x| x.checked_add(d as u64)) {
                Some(x) => v = x,
                None => over = true,
            }
        }
    }
    if over || v > i64::MAX as u64 {
        return None;
    }
    Some(v as i64)
}

// ---------- files & chunks ----------
#[inline]
fn memchr(c: u8, d: &[u8], from: usize) -> Option<usize> {
    d[from..].iter().position(|&x| x == c).map(|p| p + from)
}
#[inline]
fn for_lines(d: &[u8], mut f: impl FnMut(&[u8])) {
    let mut s = 0;
    let n = d.len();
    while s < n {
        let e = match memchr(b'\n', d, s) {
            Some(p) => p,
            None => n,
        };
        f(&d[s..e]);
        s = e + 1;
    }
}

// ---------- records ----------
#[derive(Clone, Copy)]
struct RawEv {
    eid: u128,
    person: u128,
    ingest: i64,
    ts: i64,
    price: u32,
    cat: u16,
    valid: bool,
    typ: u8,
}
#[derive(Clone, Copy)]
pub struct Ev {
    pub person: u128,
    pub ts: i64,
    pub price: u32,
    pub cat: u16,
    pub typ: u8,
}
#[derive(Clone, Copy)]
pub struct Ck {
    pub person: u128,
    pub ts: i64,
    pub cid: u128,
    pub camp: u32,
}

struct Lookup {
    short: KeyMap,
    long: HashMap<Vec<u8>, u32>,
}
impl Lookup {
    fn get(&self, s: &[u8]) -> Option<u32> {
        if s.len() <= 15 {
            self.short.get(key(s)).map(|v| v as u32)
        } else {
            self.long.get(s).copied()
        }
    }
}

fn read_small(path: &Path) -> Vec<u8> {
    std::fs::read(path).unwrap_or_default()
}

fn parse_manifest(d: &[u8]) -> (i64, i64) {
    let s = String::from_utf8_lossy(d);
    let get = |k: &str| -> i64 {
        let i = s.find(k).expect("manifest key");
        let rest = &s[i + k.len()..];
        let c = rest.find(':').unwrap();
        let rest = &rest[c + 1..];
        let q1 = rest.find('"').unwrap();
        let rest = &rest[q1 + 1..];
        let q2 = rest.find('"').unwrap();
        parse_ts(rest[..q2].as_bytes()).expect("manifest ts")
    };
    (get("\"window_start\""), get("\"window_end\""))
}

fn main() {
    let t0 = std::time::Instant::now();
    let args: Vec<String> = std::env::args().collect();
    let data = PathBuf::from(
        std::env::var("DATA_DIR").unwrap_or_else(|_| args.get(1).cloned().unwrap_or("/data".into())),
    );
    let outdir = PathBuf::from(
        std::env::var("OUTPUT_DIR").unwrap_or_else(|_| args.get(2).cloned().unwrap_or("/output".into())),
    );
    let (ws, we) = parse_manifest(&read_small(&data.join("manifest.json")));

    // products / categories
    let mut cat_names: Vec<Vec<u8>> = vec![];
    let mut cat_idx: HashMap<Vec<u8>, u16> = HashMap::new();
    let mut prod = Lookup { short: KeyMap::new(1024), long: HashMap::new() };
    let pd = read_small(&data.join("products.csv"));
    let mut first = true;
    for_lines(&pd, |l| {
        if first {
            first = false;
            return;
        }
        if let Some([p, c]) = split::<2>(l) {
            if p.is_empty() {
                return;
            }
            let ci = *cat_idx.entry(c.to_vec()).or_insert_with(|| {
                cat_names.push(c.to_vec());
                (cat_names.len() - 1) as u16
            });
            if p.len() <= 15 {
                prod.short.insert(key(p), ci as u128);
            } else {
                prod.long.insert(p.to_vec(), ci as u32);
            }
        }
    });
    let unknown = *cat_idx.entry(b"unknown".to_vec()).or_insert_with(|| {
        cat_names.push(b"unknown".to_vec());
        (cat_names.len() - 1) as u16
    });
    let mut order: Vec<usize> = (0..cat_names.len()).collect();
    order.sort_by(|&a, &b| cat_names[a].cmp(&cat_names[b]));
    let mut cat_rank = vec![0u16; cat_names.len()];
    for (r, &c) in order.iter().enumerate() {
        cat_rank[c] = r as u16;
    }

    // campaigns
    let mut camp_ids: Vec<Vec<u8>> = vec![];
    let mut camp_ch: Vec<Vec<u8>> = vec![];
    let mut camps = Lookup { short: KeyMap::new(1024), long: HashMap::new() };
    let cd = read_small(&data.join("campaigns.csv"));
    let mut first = true;
    for_lines(&cd, |l| {
        if first {
            first = false;
            return;
        }
        if let Some([c, ch]) = split::<2>(l) {
            if c.is_empty() {
                return;
            }
            let i = camp_ids.len() as u32;
            camp_ids.push(c.to_vec());
            camp_ch.push(ch.to_vec());
            if c.len() <= 15 {
                camps.short.insert(key(c), i as u128);
            } else {
                camps.long.insert(c.to_vec(), i);
            }
        }
    });
    let ncamp = camp_ids.len();

    // ---------- links ----------
    let lsrc = Source::open(&data.join("links"), 4 << 20);
    let pairs: Vec<Vec<(u128, u128)>> = lsrc.run(|d| {
            let mut v = vec![];
            each_record::<2>(d, |[a, b]| {
                if !a.is_empty() && !b.is_empty() {
                    v.push((key(a), key(b)));
                }
            });
            v
        });
    let mut idx: FxMap<u128, u32> = FxMap::default();
    let mut keys: Vec<u128> = vec![];
    let mut parent: Vec<u32> = vec![];
    fn find(p: &mut [u32], mut x: u32) -> u32 {
        while p[x as usize] != x {
            let g = p[p[x as usize] as usize];
            p[x as usize] = g;
            x = g;
        }
        x
    }
    for v in &pairs {
        for &(a, b) in v {
            let ia = *idx.entry(a).or_insert_with(|| {
                keys.push(a);
                parent.push(keys.len() as u32 - 1);
                keys.len() as u32 - 1
            });
            let ib = *idx.entry(b).or_insert_with(|| {
                keys.push(b);
                parent.push(keys.len() as u32 - 1);
                keys.len() as u32 - 1
            });
            let ra = find(&mut parent, ia);
            let rb = find(&mut parent, ib);
            if ra != rb {
                parent[ra.max(rb) as usize] = ra.min(rb);
            }
        }
    }
    let n = keys.len();
    let mut minkey: Vec<u128> = keys.clone();
    for i in 0..n {
        let r = find(&mut parent, i as u32) as usize;
        if key_cmp(keys[i], minkey[r]) == std::cmp::Ordering::Less {
            minkey[r] = keys[i];
        }
    }
    let mut person_of = KeyMap::new(n);
    for i in 0..n {
        let r = find(&mut parent, i as u32) as usize;
        if minkey[r] != keys[i] {
            person_of.insert(keys[i], minkey[r]);
        }
    }
    drop(idx);
    eprintln!("links: {} ids, {:?}", n, t0.elapsed());

    // ---------- events ----------
    let esrc = Source::open(&data.join("events"), 8 << 20);
    let parsed: Vec<Vec<Vec<RawEv>>> = esrc.run(|d| {
            let mut b: Vec<Vec<RawEv>> =
                (0..NB).map(|_| Vec::with_capacity(d.len() / 90 / NB * 2 + 4)).collect();
            each_record::<7>(d, |[eid, uid, ts, ing, typ, pid, price]| {
                if eid.is_empty() {
                    return;
                }
                let ek = key(eid);
                let ingest = parse_ts(ing).unwrap_or(i64::MAX);
                let tsv = parse_ts(ts);
                let t = match typ {
                    b"view" => 0u8,
                    b"cart" => 1,
                    b"purchase" => 2,
                    _ => 255,
                };
                let mut valid = !uid.is_empty() && t != 255 && !pid.is_empty();
                let tsv = match tsv {
                    Some(x) if x >= ws && x < we => x,
                    _ => {
                        valid = false;
                        0
                    }
                };
                let mut pr: u32 = 0;
                if valid && t == 2 {
                    if price.is_empty() {
                        valid = false;
                    } else {
                        let mut v: u64 = 0;
                        for &c in price {
                            let dd = c.wrapping_sub(b'0');
                            if dd >= 10 {
                                valid = false;
                                break;
                            }
                            v = v.saturating_mul(10).saturating_add(dd as u64);
                        }
                        if valid && !(1..=10_000_000).contains(&v) {
                            valid = false;
                        }
                        pr = v.min(u32::MAX as u64) as u32;
                    }
                }
                if valid && ingest != i64::MAX && ingest - tsv > DAY {
                    valid = false;
                }
                let (pk, cat) = if valid {
                    (key(uid), prod.get(pid).map(|x| x as u16).unwrap_or(unknown))
                } else {
                    (0, 0)
                };
                b[bucket(ek)].push(RawEv { eid: ek, person: pk, ingest, ts: tsv, price: pr, cat, valid, typ: t });
            });
            b
        });
    eprintln!("events parsed {:?}", t0.elapsed());
    let deduped: Vec<Vec<Vec<Ev>>> = par_map(NB, |bi| {
            let total: usize = parsed.iter().map(|c| c[bi].len()).sum();
            let mut v: Vec<RawEv> = Vec::with_capacity(total);
            for c in &parsed {
                v.extend_from_slice(&c[bi]);
            }
            v.sort_unstable_by_key(|r| (r.eid, r.ingest));
            let mut out: Vec<Vec<Ev>> = (0..NB).map(|_| Vec::with_capacity(total / NB * 2 + 4)).collect();
            // keep the first copy of each eid; compact valid kept copies to the front
            let mut w = 0;
            let mut i = 0;
            while i < v.len() {
                let r = v[i];
                if r.valid {
                    v[w] = r;
                    w += 1;
                }
                let mut j = i + 1;
                while j < v.len() && v[j].eid == r.eid {
                    j += 1;
                }
                i = j;
            }
            const PF: usize = 16;
            for i in 0..w {
                if i + PF < w {
                    person_of.prefetch(v[i + PF].person);
                }
                let r = v[i];
                let p = person_of.get_or_self(r.person);
                out[bucket(p)].push(Ev { person: p, ts: r.ts, price: r.price, cat: r.cat, typ: r.typ });
            }
            out
        });
    drop(parsed);
    eprintln!("events dedup {:?}", t0.elapsed());

    // ---------- clicks ----------
    let csrc = Source::open(&data.join("clicks"), 8 << 20);
    let cl: Vec<Vec<Vec<Ck>>> = csrc.run(|d| {
            let mut flat: Vec<Ck> = Vec::with_capacity(d.len() / 50);
            each_record::<4>(d, |[cid, uid, camp, ts]| {
                if cid.is_empty() || uid.is_empty() {
                    return;
                }
                let Some(t) = parse_ts(ts) else { return };
                if t < ws - WEEK || t >= we {
                    return;
                }
                let Some(ci) = camps.get(camp) else { return };
                flat.push(Ck { person: key(uid), ts: t, cid: key(cid), camp: ci });
            });
            let mut b: Vec<Vec<Ck>> = (0..NB).map(|_| Vec::with_capacity(flat.len() / NB * 2 + 4)).collect();
            const PF: usize = 16;
            for i in 0..flat.len() {
                if i + PF < flat.len() {
                    person_of.prefetch(flat[i + PF].person);
                }
                let mut c = flat[i];
                c.person = person_of.get_or_self(c.person);
                b[bucket(c.person)].push(c);
            }
            b
        });
    eprintln!("clicks parsed {:?}", t0.elapsed());

    // ---------- per person ----------
    let day0 = ws.div_euclid(DAY);
    let ndays = ((we - 1).div_euclid(DAY) - day0 + 1) as usize;
    let ncols = ncamp + 1; // organic = ncamp
    let results: Vec<(Vec<out::UserRow>, Vec<[i64; 4]>)> = par_map(NB, |bi| {
            let mut evs: Vec<Ev> = Vec::new();
            for d in &deduped {
                evs.extend_from_slice(&d[bi]);
            }
            let mut cks: Vec<Ck> = Vec::new();
            for c in &cl {
                cks.extend_from_slice(&c[bi]);
            }
            evs.sort_unstable_by_key(|e| (e.person, e.ts));
            cks.sort_unstable_by(|a, b| (a.person, a.ts).cmp(&(b.person, b.ts)).then(key_cmp(a.cid, b.cid)));
            let mut camp_acc = vec![[0i64; 4]; ncols * ndays];
            let mut rows: Vec<out::UserRow> = Vec::new();
            let mut catsum = vec![0i64; cat_rank.len()];
            let mut touched: Vec<u16> = vec![];
            let mut starts: Vec<i64> = vec![];
            let mut cc: Vec<(u32, i64)> = vec![];
            let (mut i, mut j) = (0usize, 0usize);
            while i < evs.len() {
                let p = evs[i].person;
                let mut ie = i;
                while ie < evs.len() && evs[ie].person == p {
                    ie += 1;
                }
                while j < cks.len() && cks[j].person < p {
                    j += 1;
                }
                let mut je = j;
                while je < cks.len() && cks[je].person == p {
                    je += 1;
                }
                let pc = &cks[j..je];
                let pe = &evs[i..ie];
                starts.clear();
                let mut cur_day: i64 = i64::MIN;
                let (mut d_sess, mut d_ev, mut d_rev) = (0i64, 0i64, 0i64);
                let mut cp = 0usize;
                let flush = |cur_day: i64,
                             d_sess: i64,
                             d_ev: i64,
                             d_rev: i64,
                             touched: &mut Vec<u16>,
                             catsum: &mut Vec<i64>,
                             rows: &mut Vec<out::UserRow>| {
                    if d_sess == 0 {
                        return;
                    }
                    let mut best: Option<u16> = None;
                    for &c in touched.iter() {
                        let s = catsum[c as usize];
                        best = match best {
                            None => Some(c),
                            Some(b) => {
                                let sb = catsum[b as usize];
                                if s > sb || (s == sb && cat_rank[c as usize] < cat_rank[b as usize]) {
                                    Some(c)
                                } else {
                                    Some(b)
                                }
                            }
                        };
                    }
                    for &c in touched.iter() {
                        catsum[c as usize] = 0;
                    }
                    touched.clear();
                    rows.push(out::UserRow {
                        person: p,
                        day: cur_day as i32,
                        sessions: d_sess,
                        events: d_ev,
                        revenue: d_rev,
                        top: best,
                    });
                };
                let mut k = 0;
                while k < pe.len() {
                    let start = pe[k].ts;
                    let mut ke = k + 1;
                    while ke < pe.len() && pe[ke].ts - pe[ke - 1].ts <= GAP {
                        ke += 1;
                    }
                    let mut rev = 0i64;
                    let mut purch = false;
                    let sday = start.div_euclid(DAY);
                    if sday != cur_day {
                        flush(cur_day, d_sess, d_ev, d_rev, &mut touched, &mut catsum, &mut rows);
                        cur_day = sday;
                        d_sess = 0;
                        d_ev = 0;
                        d_rev = 0;
                    }
                    for e in &pe[k..ke] {
                        if e.typ == 2 {
                            purch = true;
                            rev += e.price as i64;
                            if catsum[e.cat as usize] == 0 {
                                touched.push(e.cat);
                            }
                            catsum[e.cat as usize] += e.price as i64;
                        }
                    }
                    d_sess += 1;
                    d_ev += (ke - k) as i64;
                    d_rev += rev;
                    while cp < pc.len() && pc[cp].ts <= start {
                        cp += 1;
                    }
                    let mut camp = ncamp;
                    if cp > 0 {
                        let t = pc[cp - 1].ts;
                        if start - t <= WEEK {
                            let mut q = cp - 1;
                            while q > 0 && pc[q - 1].ts == t {
                                q -= 1;
                            }
                            camp = pc[q].camp as usize;
                        }
                    }
                    let di = (sday - day0) as usize;
                    let a = &mut camp_acc[camp * ndays + di];
                    a[0] += 1;
                    a[1] += purch as i64;
                    a[2] += rev;
                    starts.push(start);
                    k = ke;
                }
                flush(cur_day, d_sess, d_ev, d_rev, &mut touched, &mut catsum, &mut rows);
                if !pc.is_empty() {
                    cc.clear();
                    cc.extend(pc.iter().map(|c| (c.camp, c.ts)));
                    cc.sort_unstable();
                    let mut a = 0;
                    while a < cc.len() {
                        let camp = cc[a].0;
                        let mut b = a;
                        while b < cc.len() && cc[b].0 == camp {
                            b += 1;
                        }
                        let mut x = a;
                        while x < b {
                            let lo = cc[x].1;
                            let mut hi = lo + WEEK;
                            let mut y = x + 1;
                            while y < b && cc[y].1 <= hi {
                                hi = hi.max(cc[y].1 + WEEK);
                                y += 1;
                            }
                            let mut s = starts.partition_point(|&t| t < lo);
                            let e = starts.partition_point(|&t| t <= hi);
                            while s < e {
                                let d = starts[s].div_euclid(DAY);
                                let dend = (d + 1) * DAY;
                                let m = s + starts[s..e].partition_point(|&t| t < dend);
                                camp_acc[camp as usize * ndays + (d - day0) as usize][3] += (m - s) as i64;
                                s = m;
                            }
                            x = y;
                        }
                        a = b;
                    }
                }
                i = ie;
                j = je;
            }
            (rows, camp_acc)
        });
    eprintln!("sessions {:?}", t0.elapsed());
    let mut camp_acc = vec![[0i64; 4]; ncols * ndays];
    let mut all_rows: Vec<Vec<out::UserRow>> = vec![];
    for (r, c) in results {
        for (x, y) in camp_acc.iter_mut().zip(c.iter()) {
            for q in 0..4 {
                x[q] += y[q];
            }
        }
        all_rows.push(r);
    }
    let mut camp_rows: Vec<out::CampRow> = vec![];
    for c in 0..ncols {
        for d in 0..ndays {
            let a = camp_acc[c * ndays + d];
            if a[0] > 0 || a[3] > 0 {
                let (id, ch): (&[u8], &[u8]) =
                    if c == ncamp { (b"organic", b"organic") } else { (&camp_ids[c], &camp_ch[c]) };
                camp_rows.push(out::CampRow {
                    id: id.to_vec(),
                    channel: ch.to_vec(),
                    day: (day0 + d as i64) as i32,
                    sessions: a[0],
                    purchasing: a[1],
                    revenue: a[2],
                    assisted: if c == ncamp { 0 } else { a[3] },
                });
            }
        }
    }
    out::write_all(&outdir, all_rows, camp_rows, &cat_names);
    eprintln!("done {:?}", t0.elapsed());
}
