// Shopping Sessions pipeline: clean, link, sessionize, attribute, aggregate.
use std::collections::HashMap;
use std::hash::{BuildHasherDefault, Hasher};
use std::path::{Path, PathBuf};
use std::sync::Mutex;

mod alloc;
mod out;
#[global_allocator]
static GLOBAL: alloc::Huge = alloc::Huge;
pub mod par;
use par::{each_record, par_map, Source};

const SEC: i64 = 1000;
const MIN: i64 = 60 * SEC;
const HOUR: i64 = 60 * MIN;
const DAY: i64 = 24 * HOUR;
const WEEK: i64 = 7 * DAY;
const GAP: i64 = 30 * MIN;
const NBBITS: u32 = 10;
const NB: usize = 1 << NBBITS; // hash buckets
static PT: [std::sync::atomic::AtomicU64; 4] = [const { std::sync::atomic::AtomicU64::new(0) }; 4];

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
    (mix(v) >> (64 - NBBITS)) as usize
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

// ---------- open-addressing u64 -> u64 map (key 0 = empty) ----------
pub struct KeyMap64 {
    mask: usize,
    slots: Vec<[u64; 2]>,
}
impl KeyMap64 {
    pub fn new(n: usize) -> KeyMap64 {
        let cap = (n * 3 / 2 + 16).next_power_of_two();
        KeyMap64 { mask: cap - 1, slots: vec![[0, 0]; cap] }
    }
    #[inline]
    fn idx(&self, k: u64) -> usize {
        (mix(k as u128) as usize) & self.mask
    }
    pub fn insert(&mut self, k: u64, v: u64) {
        let mut i = self.idx(k);
        loop {
            let s = &mut self.slots[i];
            if s[0] == 0 || s[0] == k {
                *s = [k, v];
                return;
            }
            i = (i + 1) & self.mask;
        }
    }
    #[inline]
    pub fn prefetch(&self, k: u64) {
        #[cfg(target_arch = "x86_64")]
        unsafe {
            use std::arch::x86_64::{_MM_HINT_T0, _mm_prefetch};
            _mm_prefetch(self.slots.as_ptr().add(self.idx(k)) as *const i8, _MM_HINT_T0);
        }
    }
    #[inline]
    pub fn get_or_self(&self, k: u64) -> u64 {
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

/// Exact 8-byte key for event IDs: up to 9 bytes in 1..=127 are packed 7 bits
/// each (left-aligned, top bit 0); anything else is interned with the top bit set.
static ELONG: Mutex<Option<HashMap<Vec<u8>, u64>>> = Mutex::new(None);
#[cfg(all(target_arch = "x86_64", target_feature = "avx512bw", target_feature = "avx512vl", target_feature = "bmi2"))]
#[inline(always)]
pub fn ekey(s: &[u8]) -> u64 {
    use std::arch::x86_64::*;
    let n = s.len();
    if n <= 9 && n > 0 {
        unsafe {
            let v = _mm_maskz_loadu_epi8(((1u32 << n) - 1) as u16, s.as_ptr() as *const i8);
            // every byte must be in 1..=127
            let nz = _mm_test_epi8_mask(v, v) as u32;
            let hi = _mm_movemask_epi8(v) as u32;
            if nz == (1u32 << n) - 1 && hi == 0 {
                let w: [u64; 2] = std::mem::transmute(v);
                // 8 bytes x 7 bits = 56 bits, then the 9th byte on top: 63 bits
                return _pext_u64(w[0], 0x7F7F_7F7F_7F7F_7F7F) | ((w[1] & 0x7F) << 56);
            }
        }
    }
    ekey_long(s)
}
#[cfg(not(all(target_arch = "x86_64", target_feature = "avx512bw", target_feature = "avx512vl", target_feature = "bmi2")))]
#[inline(always)]
pub fn ekey(s: &[u8]) -> u64 {
    let n = s.len();
    if n <= 9 && n > 0 {
        let mut v: u64 = 0;
        let mut bad = 0u8;
        for &b in s {
            bad |= (b == 0 || b > 127) as u8;
            v = (v << 7) | b as u64;
        }
        if bad == 0 {
            return v << (7 * (9 - n));
        }
    }
    ekey_long(s)
}
#[cold]
fn ekey_long(s: &[u8]) -> u64 {
    let mut g = ELONG.lock().unwrap();
    let m = g.get_or_insert_with(HashMap::new);
    let next = (1u64 << 63) | (m.len() as u64 + 1);
    *m.entry(s.to_vec()).or_insert(next)
}

/// Order-preserving exact 8-byte key for user and click IDs: up to 9 bytes in
/// 1..=127 are packed 7 bits each, left-aligned, so u64 order is byte order.
/// Anything else is interned and gets the top bit set (compare those by bytes).
static ULONG: Mutex<Option<(HashMap<Vec<u8>, u64>, Vec<Vec<u8>>)>> = Mutex::new(None);
#[cfg(all(target_arch = "x86_64", target_feature = "avx512bw", target_feature = "avx512vl", target_feature = "bmi2"))]
#[inline(always)]
pub fn ukey(s: &[u8]) -> u64 {
    use std::arch::x86_64::*;
    let n = s.len();
    if n <= 9 && n > 0 {
        unsafe {
            let v = _mm_maskz_loadu_epi8(((1u32 << n) - 1) as u16, s.as_ptr() as *const i8);
            let nz = _mm_test_epi8_mask(v, v) as u32;
            let hi = _mm_movemask_epi8(v) as u32;
            if nz == (1u32 << n) - 1 && hi == 0 {
                let w: [u64; 2] = std::mem::transmute(v);
                return (_pext_u64(w[0].swap_bytes(), 0x7F7F_7F7F_7F7F_7F7F) << 7) | (w[1] & 0x7F);
            }
        }
    }
    ukey_long(s)
}
#[allow(dead_code)]
#[inline(always)]
pub fn ukey_scalar(s: &[u8]) -> u64 {
    let n = s.len();
    if n <= 9 && n > 0 {
        let mut v: u64 = 0;
        let mut bad = 0u8;
        for i in 0..9 {
            let b = if i < n { s[i] } else { 0 };
            bad |= (i < n && (b == 0 || b > 127)) as u8;
            v = (v << 7) | b as u64;
        }
        if bad == 0 {
            return v;
        }
    }
    ukey_long(s)
}
#[cfg(not(all(target_arch = "x86_64", target_feature = "avx512bw", target_feature = "avx512vl", target_feature = "bmi2")))]
#[inline(always)]
pub fn ukey(s: &[u8]) -> u64 {
    ukey_scalar(s)
}
#[cold]
fn ukey_long(s: &[u8]) -> u64 {
    let mut g = ULONG.lock().unwrap();
    let (m, l) = g.get_or_insert_with(|| (HashMap::new(), Vec::new()));
    if let Some(&k) = m.get(s) {
        return k;
    }
    let k = (1u64 << 63) | l.len() as u64;
    l.push(s.to_vec());
    m.insert(s.to_vec(), k);
    k
}
/// Append the bytes of a user key to `out`.
#[inline]
pub fn ukey_put(k: u64, out: &mut Vec<u8>) {
    if k >> 63 == 1 {
        out.extend_from_slice(&ukey_bytes(k));
        return;
    }
    for i in 0..9 {
        let b = ((k >> (7 * (8 - i))) & 0x7F) as u8;
        if b == 0 {
            break;
        }
        out.push(b);
    }
}
pub fn ukey_bytes(k: u64) -> Vec<u8> {
    if k >> 63 == 1 {
        return ULONG.lock().unwrap().as_ref().unwrap().1[(k & !(1u64 << 63)) as usize].clone();
    }
    let mut out = Vec::with_capacity(9);
    for i in 0..9 {
        let b = ((k >> (7 * (8 - i))) & 0x7F) as u8;
        if b == 0 {
            break;
        }
        out.push(b);
    }
    out
}
/// Whether any ID needed interning (then u64 order is not byte order for those).
pub fn any_interned() -> bool {
    ULONG.lock().unwrap().as_ref().map(|x| !x.1.is_empty()).unwrap_or(false)
}
#[inline]
pub fn ukey_cmp(a: u64, b: u64) -> std::cmp::Ordering {
    if (a | b) >> 63 == 1 {
        ukey_bytes(a).cmp(&ukey_bytes(b))
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
// days from 1970-01-01 to Jan 1 of each year 0..=9999, built at compile time
const fn year_table() -> [i32; 10000] {
    let mut t = [0i32; 10000];
    let mut y = 0;
    while y < 10000 {
        let yy = y as i64 - 1;
        // days_from_civil(y, 1, 1)
        let (yv, m, d) = (y as i64, 1i64, 1i64);
        let yv = if m <= 2 { yv - 1 } else { yv };
        let era = if yv >= 0 { yv } else { yv - 399 } / 400;
        let yoe = yv - era * 400;
        let mp = (m + 9) % 12;
        let doy = (153 * mp + 2) / 5 + d - 1;
        let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
        t[y] = (era * 146097 + doe - 719468) as i32;
        let _ = yy;
        y += 1;
    }
    t
}
static YEAR_DAYS: [i32; 10000] = year_table();
const CUM: [[i16; 13]; 2] = [
    [0, 31, 59, 90, 120, 151, 181, 212, 243, 273, 304, 334, 365],
    [0, 31, 60, 91, 121, 152, 182, 213, 244, 274, 305, 335, 366],
];

#[allow(dead_code)]
#[inline(always)]
fn civil_scalar(s: &[u8]) -> Option<i64> {
    // YYYY-MM-DDTHH:MM:SS (callers guarantee len >= 20)
    let s: &[u8; 19] = s[..19].try_into().unwrap();
    let mut b = [0u8; 19];
    let mut bad = 0u8;
    const DIG: [usize; 14] = [0, 1, 2, 3, 5, 6, 8, 9, 11, 12, 14, 15, 17, 18];
    for &p in DIG.iter() {
        let v = s[p].wrapping_sub(b'0');
        b[p] = v;
        bad |= (v > 9) as u8;
    }
    bad |= (s[4] != b'-') as u8 | (s[7] != b'-') as u8 | (s[10] != b'T') as u8 | (s[13] != b':') as u8 | (s[16] != b':') as u8;
    if bad != 0 {
        return None;
    }
    let y = b[0] as usize * 1000 + b[1] as usize * 100 + b[2] as usize * 10 + b[3] as usize;
    let mo = (b[5] * 10 + b[6]) as usize;
    let d = (b[8] * 10 + b[9]) as i64;
    let h = (b[11] * 10 + b[12]) as i64;
    let mi = (b[14] * 10 + b[15]) as i64;
    let se = (b[17] * 10 + b[18]) as i64;
    if mo == 0 || mo > 12 || d == 0 || h > 23 || mi > 59 || se > 59 {
        return None;
    }
    let leap = ((y % 4 == 0 && y % 100 != 0) || y % 400 == 0) as usize;
    let c = &CUM[leap];
    if d > (c[mo] - c[mo - 1]) as i64 {
        return None;
    }
    let days = YEAR_DAYS[y] as i64 + c[mo - 1] as i64 + d - 1;
    Some(((days * 24 + h) * 60 + mi) * 60_000 + se * 1000)
}

#[cfg(not(all(target_arch = "x86_64", target_feature = "avx512bw", target_feature = "avx512vl")))]
#[inline(always)]
fn civil(s: &[u8]) -> Option<i64> {
    civil_scalar(s)
}

/// SIMD YYYY-MM-DDTHH:MM:SS (bytes 0..19; callers guarantee len >= 20).
#[cfg(all(target_arch = "x86_64", target_feature = "avx512bw", target_feature = "avx512vl"))]
#[inline(always)]
fn civil(s: &[u8]) -> Option<i64> {
    use std::arch::x86_64::*;
    unsafe {
        // 32-byte masked load of the first 19 bytes
        let v = _mm256_maskz_loadu_epi8((1u32 << 19) - 1, s.as_ptr() as *const i8);
        let zero = _mm256_set1_epi8(b'0' as i8);
        let d = _mm256_sub_epi8(v, zero);
        const DIGM: u32 = 0b110_1101_1011_0110_1111; // positions 0-3,5,6,8,9,11,12,14,15,17,18
        let lt = _mm256_cmplt_epu8_mask(d, _mm256_set1_epi8(10));
        let tmpl = _mm256_setr_epi8(
            0, 0, 0, 0, b'-' as i8, 0, 0, b'-' as i8, 0, 0, b'T' as i8, 0, 0, b':' as i8, 0, 0, b':' as i8, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
        );
        const SEPM: u32 = (1 << 4) | (1 << 7) | (1 << 10) | (1 << 13) | (1 << 16);
        let eq = _mm256_cmpeq_epi8_mask(v, tmpl);
        if (lt & DIGM) != DIGM || (eq & SEPM) != SEPM {
            return None;
        }
        // gather the 14 digits of the low 16 bytes + bytes 17,18 into pairs
        let lo = _mm256_castsi256_si128(d);
        let hi = _mm256_extracti128_si256(d, 1); // bytes 16..31
        // lo bytes: 0..15 ; we need pairs (0,1)(2,3)(5,6)(8,9)(11,12)(14,15)
        let shuf = _mm_setr_epi8(0, 1, 2, 3, 5, 6, 8, 9, 11, 12, 14, 15, -1, -1, -1, -1);
        let g = _mm_shuffle_epi8(lo, shuf);
        let w = _mm_setr_epi8(10, 1, 10, 1, 10, 1, 10, 1, 10, 1, 10, 1, 0, 0, 0, 0);
        let p = _mm_maddubs_epi16(g, w); // u16 x6: Y1 Y2 MO D H MI
        let a: [u16; 8] = std::mem::transmute(p);
        let hb: [u8; 16] = std::mem::transmute(hi);
        let se = (hb[1] * 10 + hb[2]) as i64; // bytes 17,18
        let y = a[0] as usize * 100 + a[1] as usize;
        let mo = a[2] as usize;
        let dd = a[3] as i64;
        let h = a[4] as i64;
        let mi = a[5] as i64;
        if mo.wrapping_sub(1) >= 12 || dd == 0 || h > 23 || mi > 59 || se > 59 {
            return None;
        }
        let leap = ((y % 4 == 0 && y % 100 != 0) || y % 400 == 0) as usize;
        let c = &CUM[leap];
        if dd > (c[mo] - c[mo - 1]) as i64 {
            return None;
        }
        let days = YEAR_DAYS[y] as i64 + c[mo - 1] as i64 + dd - 1;
        Some(((days * 24 + h) * 60 + mi) * 60_000 + se * 1000)
    }
}

/// F3 digits (1..=16 bytes) to a number; None if any byte is not a digit.
#[cfg(all(target_arch = "x86_64", target_feature = "avx512bw", target_feature = "avx512vl"))]
#[inline(always)]
fn digits16(s: &[u8]) -> Option<u64> {
    use std::arch::x86_64::*;
    let n = s.len();
    unsafe {
        // right-align: lane i holds s[i - (16 - n)], leading lanes are 0
        let k = 16 - n;
        let mask: u16 = (((1u32 << n) - 1) << k) as u16;
        let v = _mm_maskz_loadu_epi8(mask, s.as_ptr().wrapping_sub(k) as *const i8);
        let d = _mm_sub_epi8(v, _mm_set1_epi8(b'0' as i8));
        let d = _mm_maskz_mov_epi8(mask, d);
        let ok = _mm_cmplt_epu8_mask(d, _mm_set1_epi8(10));
        if ok != 0xFFFF {
            return None;
        }
        let p1 = _mm_maddubs_epi16(d, _mm_setr_epi8(10, 1, 10, 1, 10, 1, 10, 1, 10, 1, 10, 1, 10, 1, 10, 1));
        let p2 = _mm_madd_epi16(p1, _mm_setr_epi16(100, 1, 100, 1, 100, 1, 100, 1));
        let p3 = _mm_packus_epi32(p2, p2);
        let p4 = _mm_madd_epi16(p3, _mm_setr_epi16(10000, 1, 10000, 1, 0, 0, 0, 0));
        let r: [u32; 4] = std::mem::transmute(p4);
        Some(r[0] as u64 * 100_000_000 + r[1] as u64)
    }
}
#[cfg(not(all(target_arch = "x86_64", target_feature = "avx512bw", target_feature = "avx512vl")))]
#[inline(always)]
fn digits16(s: &[u8]) -> Option<u64> {
    let mut v: u64 = 0;
    let mut bad = 0u8;
    for &c in s {
        let d = c.wrapping_sub(b'0');
        bad |= (d > 9) as u8;
        v = v.wrapping_mul(10).wrapping_add(d as u64);
    }
    if bad != 0 { None } else { Some(v) }
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
    // F3: digits only. Up to 18 digits cannot overflow.
    if n <= 16 {
        return digits16(s).map(|v| v as i64);
    }
    if n <= 18 {
        let mut v: u64 = 0;
        let mut bad = 0u8;
        for &c in s {
            let d = c.wrapping_sub(b'0');
            bad |= (d > 9) as u8;
            v = v.wrapping_mul(10).wrapping_add(d as u64);
        }
        return if bad != 0 { None } else { Some(v as i64) };
    }
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

// ---------- partitioned arrays ----------
pub struct Parts<T> {
    data: Vec<T>,
    off: Vec<u32>,
}
impl<T: Copy> Parts<T> {
    #[inline]
    pub fn get(&self, b: usize) -> &[T] {
        &self.data[self.off[b] as usize..self.off[b + 1] as usize]
    }
    /// Scatter `flat` into NB buckets given each element's bucket and the counts.
    pub fn from_keys(flat: &[T], ks: &[u16], counts: &[u32]) -> Parts<T> {
        let mut cnt = vec![0u32; NB + 1];
        for i in 0..NB {
            cnt[i + 1] = cnt[i] + counts[i];
        }
        let off = cnt.clone();
        let mut data: Vec<T> = Vec::with_capacity(flat.len());
        unsafe { data.set_len(flat.len()) };
        for (x, &k) in flat.iter().zip(ks.iter()) {
            let c = &mut cnt[k as usize];
            unsafe { *data.get_unchecked_mut(*c as usize) = *x };
            *c += 1;
        }
        Parts { data, off }
    }
    /// Counting sort of `flat` into NB buckets by `key`.
    pub fn build(flat: &[T], key: impl Fn(&T) -> usize) -> Parts<T> {
        let mut cnt = vec![0u32; NB + 1];
        let ks: Vec<u16> = flat.iter().map(|x| key(x) as u16).collect();
        for &k in &ks {
            cnt[k as usize + 1] += 1;
        }
        for i in 0..NB {
            cnt[i + 1] += cnt[i];
        }
        let off = cnt.to_vec();
        let mut data: Vec<T> = Vec::with_capacity(flat.len());
        unsafe { data.set_len(flat.len()) };
        for (x, &k) in flat.iter().zip(ks.iter()) {
            data[cnt[k as usize] as usize] = *x;
            cnt[k as usize] += 1;
        }
        Parts { data, off }
    }
}

// ---------- records ----------
#[derive(Clone, Copy)]
#[repr(C, packed(4))]
struct RawEv {
    eid: u64,
    /// user key; 0 means the copy is invalid
    person: u64,
    ingest: i64,
    ts: u32,
    /// 0 for view/cart; for a purchase (category << 24) | price
    pc: u32,
}
#[derive(Clone, Copy)]
#[repr(C, packed(4))]
pub struct Ev {
    pub person: u64,
    pub ts: u32,
    /// 0 for view/cart; for a purchase (category << 24) | price (price < 2^24)
    pub pc: u32,
}
#[derive(Clone, Copy)]
#[repr(C, packed(4))]
pub struct Ck {
    pub person: u64,
    /// ms since window_start - 7 days
    pub ts: u32,
    pub cid: u64,
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
    debug_assert!(std::mem::size_of::<RawEv>() == 32);
    let t0 = std::time::Instant::now();
    let args: Vec<String> = std::env::args().collect();
    let data = PathBuf::from(
        std::env::var("DATA_DIR").unwrap_or_else(|_| args.get(1).cloned().unwrap_or("/data".into())),
    );
    let outdir = PathBuf::from(
        std::env::var("OUTPUT_DIR").unwrap_or_else(|_| args.get(2).cloned().unwrap_or("/output".into())),
    );
    let (ws, we) = parse_manifest(&read_small(&data.join("manifest.json")));
    // event times are stored as u32 ms offsets from window_start (windows are 14 days)
    assert!(we - ws + WEEK < (1i64 << 32), "window longer than 42 days");

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
    // purchases pack the category into 8 bits next to the price
    assert!(cat_names.len() <= 255, "more than 255 categories");
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

    let build_links = || -> KeyMap64 {
    // ---------- links ----------
    let lsrc = Source::open(&data.join("links"), 4 << 20);
    let pairs: Vec<Vec<(u64, u64)>> = lsrc.run(|d| {
            let mut v = vec![];
            each_record::<2>(d, |[a, b]| {
                if !a.is_empty() && !b.is_empty() {
                    v.push((ukey(a), ukey(b)));
                }
            });
            v
        });
    eprintln!("links parsed {:?}", t0.elapsed());
    let total_pairs: usize = pairs.iter().map(|v| v.len()).sum();
    let mut idx = KeyMap::new(total_pairs);
    let mut keys: Vec<u64> = vec![];
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
            let ia = match idx.get(a as u128) {
                Some(v) => v as u32,
                None => {
                    keys.push(a);
                    parent.push(keys.len() as u32 - 1);
                    idx.insert(a as u128, (keys.len() - 1) as u128);
                    keys.len() as u32 - 1
                }
            };
            let ib = match idx.get(b as u128) {
                Some(v) => v as u32,
                None => {
                    keys.push(b);
                    parent.push(keys.len() as u32 - 1);
                    idx.insert(b as u128, (keys.len() - 1) as u128);
                    keys.len() as u32 - 1
                }
            };
            let ra = find(&mut parent, ia);
            let rb = find(&mut parent, ib);
            if ra != rb {
                parent[ra.max(rb) as usize] = ra.min(rb);
            }
        }
    }
    let n = keys.len();
    eprintln!("links uf {:?}", t0.elapsed());
    let mut minkey: Vec<u64> = keys.clone();
    for i in 0..n {
        let r = find(&mut parent, i as u32) as usize;
        if ukey_cmp(keys[i], minkey[r]) == std::cmp::Ordering::Less {
            minkey[r] = keys[i];
        }
    }
    let mut person_of = KeyMap64::new(n);
    for i in 0..n {
        let r = find(&mut parent, i as u32) as usize;
        if minkey[r] != keys[i] {
            person_of.insert(keys[i], minkey[r]);
        }
    }
    drop(idx);
    eprintln!("links: {} ids, {:?}", n, t0.elapsed());
        person_of
    };
    let (person_of, parsed) = std::thread::scope(|sc| {
        let h = sc.spawn(build_links);
    // ---------- events ----------
    let esrc = Source::open(&data.join("events"), 2 << 20);
    let parsed: Vec<Parts<RawEv>> = esrc.run(|d| {
            let mut flat: Vec<RawEv> = Vec::with_capacity(d.len() / 64 + 1024);
            let mut ks: Vec<u16> = Vec::with_capacity(d.len() / 64 + 1024);
            let mut counts = vec![0u32; NB];
            each_record::<7>(d, |[eid, uid, ts, ing, typ, pid, price]| {
                if eid.is_empty() {
                    return;
                }
                let ek = ekey(eid);
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
                    (ukey(uid), if t == 2 { prod.get(pid).map(|x| x as u16).unwrap_or(unknown) } else { 0 })
                } else {
                    (0, 0)
                };
                let b = bucket(ek as u128);
                ks.push(b as u16);
                counts[b] += 1;
                flat.push(RawEv { eid: ek, person: pk, ingest, ts: (tsv - ws) as u32, pc: if t == 2 { (cat as u32) << 24 | pr } else { 0 } });
            });
            Parts::from_keys(&flat, &ks, &counts)
        });
    eprintln!("events parsed {:?}", t0.elapsed());
        (h.join().unwrap(), parsed)
    });

    let dedup_task = |bi: usize| -> Parts<Ev> {
            let total: usize = parsed.iter().map(|c| c.get(bi).len()).sum();
            // hash dedup, two sequential passes: min ingest per eid, then emit the
            // first valid copy carrying that ingest
            let cap = (total * 3 / 2 + 16).next_power_of_two();
            let mask = cap - 1;
            thread_local! {
                static TAB: std::cell::RefCell<Vec<(u64, i64)>> = const { std::cell::RefCell::new(Vec::new()) };
            }
            let mut tab: Vec<(u64, i64)> = TAB.with(|t| std::mem::take(&mut *t.borrow_mut()));
            if tab.len() < cap {
                tab = vec![(0, 0); cap];
            }
            let tab_full = tab.len();
            let _ = tab_full;
            let mut tab_slice = std::mem::take(&mut tab);
            tab_slice.truncate(cap);
            let mut tab = tab_slice;
            let mut all: Vec<RawEv> = Vec::with_capacity(total);
            for c in parsed.iter() {
                all.extend_from_slice(c.get(bi));
            }
            let hs: Vec<u32> = all.iter().map(|r| (mix(r.eid as u128) as usize & mask) as u32).collect();
            const PFD: usize = 12;
            let pf = |tab: &Vec<(u64, i64)>, h: u32| {
                #[cfg(target_arch = "x86_64")]
                unsafe {
                    use std::arch::x86_64::{_MM_HINT_T0, _mm_prefetch};
                    _mm_prefetch(tab.as_ptr().add(h as usize) as *const i8, _MM_HINT_T0);
                }
            };
            for i in 0..all.len() {
                if i + PFD < all.len() {
                    pf(&tab, hs[i + PFD]);
                }
                let r = &all[i];
                let mut h = hs[i] as usize;
                loop {
                    let s = &mut tab[h];
                    if s.0 == 0 {
                        *s = (r.eid, r.ingest);
                        break;
                    }
                    if s.0 == r.eid {
                        if r.ingest < s.1 {
                            s.1 = r.ingest;
                        }
                        break;
                    }
                    h = (h + 1) & mask;
                }
            }
            let mut out: Vec<Ev> = Vec::with_capacity(total);
            for i in 0..all.len() {
                if i + PFD < all.len() {
                    pf(&tab, hs[i + PFD]);
                }
                let r = &all[i];
                let mut h = hs[i] as usize;
                loop {
                    let s = &mut tab[h];
                    if s.0 == r.eid {
                        if s.1 == r.ingest {
                            s.1 = i64::MIN;
                            if r.person != 0 {
                                out.push(Ev { person: r.person, ts: r.ts, pc: r.pc });
                            }
                        }
                        break;
                    }
                    h = (h + 1) & mask;
                }
            }
            tab.fill((0, 0));
            TAB.with(|t| *t.borrow_mut() = tab);
            drop(all);
            let w = out.len();
            const PF: usize = 16;
            let mut ks: Vec<u16> = Vec::with_capacity(w);
            let mut counts = vec![0u32; NB];
            for i in 0..w {
                if i + PF < w {
                    person_of.prefetch(out[i + PF].person);
                }
                let p = person_of.get_or_self(out[i].person);
                out[i].person = p;
                let b = bucket(p as u128);
                ks.push(b as u16);
                counts[b] += 1;
            }
            Parts::from_keys(&out, &ks, &counts)
        };
    let csrc = Source::open(&data.join("clicks"), 2 << 20);
    let click_task = |d: &[u8]| -> Parts<Ck> {
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
                flat.push(Ck { person: ukey(uid), ts: (t - (ws - WEEK)) as u32, cid: ukey(cid), camp: ci });
            });
            const PF: usize = 16;
            let mut ks: Vec<u16> = Vec::with_capacity(flat.len());
            let mut counts = vec![0u32; NB];
            for i in 0..flat.len() {
                if i + PF < flat.len() {
                    person_of.prefetch(flat[i + PF].person);
                }
                let p = person_of.get_or_self(flat[i].person);
                flat[i].person = p;
                let b = bucket(p as u128);
                ks.push(b as u16);
                counts[b] += 1;
            }
            Parts::from_keys(&flat, &ks, &counts)
        };
    // clicks parsing and event dedup share one pool (one barrier less)
    enum Out {
        E(Parts<Ev>),
        C(Parts<Ck>),
    }
    let nct = csrc.tasks.len();
    let mixed: Vec<Out> = par_map(nct + NB, |i| if i < nct { Out::C(csrc.run_one(i, &click_task)) } else { Out::E(dedup_task(i - nct)) });
    let mut deduped: Vec<Parts<Ev>> = Vec::with_capacity(NB);
    let mut cl: Vec<Parts<Ck>> = Vec::with_capacity(nct);
    for o in mixed {
        match o {
            Out::E(p) => deduped.push(p),
            Out::C(p) => cl.push(p),
        }
    }
    drop(parsed);

    eprintln!("events dedup {:?}", t0.elapsed());

    // ---------- clicks ----------
    eprintln!("clicks parsed {:?}", t0.elapsed());

    // ---------- per person ----------
    let day0 = ws.div_euclid(DAY);
    let ndays = ((we - 1).div_euclid(DAY) - day0 + 1) as usize;
    let ncols = ncamp + 1; // organic = ncamp
    let interned = any_interned();
    const GB: usize = 32; // buckets per task
    let _ = GB;
    // one task per person bucket, largest first (whale persons), so no thread
    // is left with a big bucket at the end; per-thread accumulators
    let sizes: Vec<usize> = (0..NB)
        .map(|bi| deduped.iter().map(|d| d.get(bi).len()).sum::<usize>() + cl.iter().map(|c| c.get(bi).len()).sum::<usize>())
        .collect();
    let mut order: Vec<usize> = (0..NB).collect();
    order.sort_by_key(|&b| std::cmp::Reverse(sizes[b]));
    // per thread: user rows, credited (sessions, purchasing, revenue), assisted
    type Acc = (Vec<out::UserRow>, Vec<(u32, u32, u64)>, Vec<u32>);
    let results: Vec<Acc> = par::par_fold(&order, || -> Acc { (Vec::new(), vec![(0, 0, 0); ncols * ndays], vec![0; ncols * ndays]) }, |st, bi| {
        let (rows, credit, assist) = (&mut st.0, &mut st.1, &mut st.2);
        {
            let tq = std::time::Instant::now();
            let nev: usize = deduped.iter().map(|d| d.get(bi).len()).sum();
            let sbits = (usize::BITS - (nev / 4).max(1).leading_zeros()).clamp(1, 16);
            let _ = sbits;
            let evs: Vec<Ev> = radix_gather(deduped.iter().map(|d| d.get(bi)), |e| h16(e.person) << 16 | (e.ts >> 16) as u64, |x, y| {
                let (hx, hy) = (h16(x.person), h16(y.person));
                if hx != hy {
                    return hx < hy;
                }
                let (xp, yp) = (x.person, y.person);
                xp < yp || (xp == yp && x.ts < y.ts)
            });
            let _ = interned;
            let cks: Vec<Ck> = radix_gather(cl.iter().map(|c| c.get(bi)), |c| h16(c.person) << 16 | (c.ts >> 16) as u64, |x, y| {
                let (hx, hy) = (h16(x.person), h16(y.person));
                if hx != hy {
                    return hx < hy;
                }
                let (xp, yp) = (x.person, y.person);
                if xp != yp {
                    return xp < yp;
                }
                let (xt, yt) = (x.ts, y.ts);
                if xt != yt {
                    return xt < yt;
                }
                ukey_cmp(x.cid, y.cid) == std::cmp::Ordering::Less
            });
            PT[0].fetch_add(tq.elapsed().as_nanos() as u64, std::sync::atomic::Ordering::Relaxed);
            let tq = std::time::Instant::now();
            // order = (12 hash bits of person, person, ts): counting sort on the
            // hash bits, then small sorts inside each sub-bucket
            PT[3].fetch_add(tq.elapsed().as_nanos() as u64, std::sync::atomic::Ordering::Relaxed);
            PT[1].fetch_add(tq.elapsed().as_nanos() as u64, std::sync::atomic::Ordering::Relaxed);
            let tq = std::time::Instant::now();
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
                while j < cks.len() && (h16(cks[j].person), cks[j].person) < (h16(p), p) {
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
                    let start = pe[k].ts as i64 + ws;
                    let mut ke = k + 1;
                    while ke < pe.len() && (pe[ke].ts - pe[ke - 1].ts) as i64 <= GAP {
                        ke += 1;
                    }
                    let mut rev = 0i64;
                    let mut purch = false;
                    let sday = start.div_euclid(DAY);
                    if sday != cur_day {
                        flush(cur_day, d_sess, d_ev, d_rev, &mut touched, &mut catsum, rows);
                        cur_day = sday;
                        d_sess = 0;
                        d_ev = 0;
                        d_rev = 0;
                    }
                    for e in &pe[k..ke] {
                        if e.pc != 0 {
                            let price = (e.pc & 0xFF_FFFF) as i64;
                            let cat = (e.pc >> 24) as u16;
                            purch = true;
                            rev += price;
                            if catsum[cat as usize] == 0 {
                                touched.push(cat);
                            }
                            catsum[cat as usize] += price;
                        }
                    }
                    d_sess += 1;
                    d_ev += (ke - k) as i64;
                    d_rev += rev;
                    while cp < pc.len() && pc[cp].ts as i64 + (ws - WEEK) <= start {
                        cp += 1;
                    }
                    let mut camp = ncamp;
                    if cp > 0 {
                        let t = pc[cp - 1].ts as i64 + (ws - WEEK);
                        if start - t <= WEEK {
                            let mut q = cp - 1;
                            while q > 0 && pc[q - 1].ts as i64 + (ws - WEEK) == t {
                                q -= 1;
                            }
                            camp = pc[q].camp as usize;
                        }
                    }
                    let di = (sday - day0) as usize;
                    let a = &mut credit[camp * ndays + di];
                    a.0 += 1;
                    a.1 += purch as u32;
                    a.2 += rev as u64;
                    starts.push(start);
                    k = ke;
                }
                flush(cur_day, d_sess, d_ev, d_rev, &mut touched, &mut catsum, rows);
                if !pc.is_empty() {
                    cc.clear();
                    cc.extend(pc.iter().map(|c| (c.camp, c.ts as i64 + (ws - WEEK))));
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
                                assist[camp as usize * ndays + (d - day0) as usize] += (m - s) as u32;
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
            PT[2].fetch_add(tq.elapsed().as_nanos() as u64, std::sync::atomic::Ordering::Relaxed);
        }
        });
    eprintln!("sessions {:?}", t0.elapsed());
    eprintln!("  sess cpu: evsort {}ms gather {}ms prep {}ms loop {}ms", PT[3].load(std::sync::atomic::Ordering::Relaxed)/1000000, PT[0].load(std::sync::atomic::Ordering::Relaxed)/1000000, PT[1].load(std::sync::atomic::Ordering::Relaxed)/1000000, PT[2].load(std::sync::atomic::Ordering::Relaxed)/1000000);
    let mut camp_acc = vec![[0i64; 4]; ncols * ndays];
    let mut all_rows: Vec<Vec<out::UserRow>> = vec![];
    for (r, c, a) in results {
        for ((x, y), z) in camp_acc.iter_mut().zip(c.iter()).zip(a.iter()) {
            x[0] += y.0 as i64;
            x[1] += y.1 as i64;
            x[2] += y.2 as i64;
            x[3] += *z as i64;
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
    // skip destructors of the large arrays: the OS reclaims them at exit
    std::process::exit(0);
}

/// LSD radix sort of (key, payload) pairs by key, over the low `bits` bits.
fn radix_sort(a: &mut Vec<(u64, u64)>, bits: u32) {
    const D: u32 = 8;
    let n = a.len();
    if n < 64 {
        a.sort_unstable_by_key(|x| x.0);
        return;
    }
    let mut b: Vec<(u64, u64)> = Vec::with_capacity(n);
    unsafe { b.set_len(n) };
    let mut shift = 0;
    while shift < bits {
        let mut cnt = [0usize; 1 << D];
        for x in a.iter() {
            cnt[((x.0 >> shift) & ((1 << D) - 1)) as usize] += 1;
        }
        if cnt.iter().any(|&c| c == n) {
            shift += D;
            continue;
        }
        let mut sum = 0;
        for c in cnt.iter_mut() {
            let t = *c;
            *c = sum;
            sum += t;
        }
        for x in a.iter() {
            let d = ((x.0 >> shift) & ((1 << D) - 1)) as usize;
            b[cnt[d]] = *x;
            cnt[d] += 1;
        }
        std::mem::swap(a, &mut b);
        shift += D;
    }
}

#[inline]
fn hk(p: u128) -> u64 {
    (mix(p) >> 24) & 0xFFF
}
fn sub_sort<T: Copy>(v: &mut Vec<T>, key: impl Fn(&T) -> u128, cmp: impl Fn(&T, &T) -> std::cmp::Ordering) {
    let n = v.len();
    let mut cnt = vec![0usize; 4097];
    let ks: Vec<u16> = v.iter().map(|x| hk(key(x)) as u16).collect();
    for &k in &ks {
        cnt[k as usize + 1] += 1;
    }
    for i in 0..4096 {
        cnt[i + 1] += cnt[i];
    }
    let starts = cnt.clone();
    let mut out: Vec<T> = Vec::with_capacity(n);
    unsafe { out.set_len(n) };
    for (x, &k) in v.iter().zip(ks.iter()) {
        out[cnt[k as usize]] = *x;
        cnt[k as usize] += 1;
    }
    for i in 0..4096 {
        let s = &mut out[starts[i]..starts[i + 1]];
        if s.len() > 1 {
            s.sort_unstable_by(&cmp);
        }
    }
    *v = out;
}

#[cfg(test)]
mod tests {
    use super::*;
    fn scalar_ts(s: &[u8]) -> Option<i64> {
        let n = s.len();
        if n == 20 && s[19] == b'Z' {
            return civil_scalar(s);
        }
        if n == 25 && (s[19] == b'+' || s[19] == b'-') && s[22] == b':' {
            let base = civil_scalar(s)?;
            let oh = d2(s, 20)?;
            let om = d2(s, 23)?;
            if oh > 23 || om > 59 {
                return None;
            }
            let off = (oh * 60 + om) * MIN;
            return Some(if s[19] == b'+' { base - off } else { base + off });
        }
        if n == 0 { return None; }
        let mut v: u128 = 0;
        for &c in s {
            if !c.is_ascii_digit() { return None; }
            v = (v * 10 + (c - b'0') as u128).min(u128::MAX / 100);
        }
        if v > i64::MAX as u128 { None } else { Some(v as i64) }
    }
    #[test]
    fn ukey_order() {
        let mut x: u64 = 0x9e37_79b9_7f4a_7c15;
        let mut rnd = || { x ^= x << 13; x ^= x >> 7; x ^= x << 17; x };
        let alpha: Vec<u8> = b"uU0129az \x01\x7f".iter().copied().chain([0x80u8, 0xC3, 0xA9, 0]).collect();
        let mut pool: Vec<Vec<u8>> = vec![];
        for _ in 0..20000 {
            let l = 1 + rnd() % 13;
            pool.push((0..l).map(|_| alpha[(rnd() % alpha.len() as u64) as usize]).collect());
        }
        let keys: Vec<u64> = pool.iter().map(|p| ukey(p)).collect();
        for _ in 0..400000 {
            let i = (rnd() % pool.len() as u64) as usize;
            let j = (rnd() % pool.len() as u64) as usize;
            assert_eq!(ukey_cmp(keys[i], keys[j]), pool[i].cmp(&pool[j]));
            assert_eq!(keys[i] == keys[j], pool[i] == pool[j]);
            assert_eq!(ukey_bytes(keys[i]), pool[i]);
            assert_eq!(keys[i], ukey_scalar(&pool[i]));
        }
    }
    #[test]
    fn fuzz_ts() {
        let mut x: u64 = 0x1234_5678_9abc_def1;
        let mut rnd = || { x ^= x << 13; x ^= x >> 7; x ^= x << 17; x };
        let alpha = b"0123456789-:TZ+ z.";
        for it in 0..3_000_000u64 {
            let base: Vec<u8> = match it % 4 {
                0 => b"2027-12-19T18:13:05Z".to_vec(),
                1 => b"2028-02-29T23:59:59+05:30".to_vec(),
                2 => b"1791071890000".to_vec(),
                _ => (0..(rnd() % 26)).map(|_| alpha[(rnd() % alpha.len() as u64) as usize]).collect(),
            };
            let mut s = base.clone();
            let muts = rnd() % 4;
            for _ in 0..muts {
                if s.is_empty() { break; }
                let i = (rnd() as usize) % s.len();
                s[i] = alpha[(rnd() % alpha.len() as u64) as usize];
            }
            // digit-only random lengths
            if it % 7 == 0 {
                let l = 1 + rnd() % 20;
                s = (0..l).map(|_| b'0' + (rnd() % 10) as u8).collect();
            }
            // pad into a bigger buffer so SIMD loads have neighbours
            let mut buf = vec![b'9'; 40];
            buf.extend_from_slice(&s);
            buf.extend_from_slice(&[b'7'; 40]);
            let sl = &buf[40..40 + s.len()];
            assert_eq!(parse_ts(sl), scalar_ts(sl), "{:?}", String::from_utf8_lossy(sl));
        }
    }
}

/// Gather the slices into one array ordered by (hk(key), then `less`), using a
/// counting sort on the 12 hash bits and insertion sort inside each sub-bucket.
#[inline]
fn sub_of(k: u128, bits: u32) -> usize {
    ((mix(k) >> 8) as usize) & ((1usize << bits) - 1)
}
fn gather_sorted<'a, T: Copy + 'a, I: Iterator<Item = &'a [T]> + Clone, K: Ord + Copy>(
    src: I,
    bits: u32,
    key: impl Fn(&T) -> u128,
    less: impl Fn(&T, &T) -> bool,
    big_key: impl Fn(&T) -> K,
    fix: bool,
) -> Vec<T> {
    let n: usize = src.clone().map(|s| s.len()).sum();
    let nsub = 1usize << bits;
    let mut ks: Vec<u32> = Vec::with_capacity(n);
    let mut cnt = vec![0u32; nsub + 1];
    for sl in src.clone() {
        for x in sl {
            let k = sub_of(key(x), bits) as u32;
            ks.push(k);
            cnt[k as usize + 1] += 1;
        }
    }
    for i in 0..nsub {
        cnt[i + 1] += cnt[i];
    }
    let starts = cnt.clone();
    let mut out: Vec<T> = Vec::with_capacity(n);
    unsafe { out.set_len(n) };
    let mut j = 0;
    for sl in src {
        for x in sl {
            let k = ks[j] as usize;
            j += 1;
            out[cnt[k] as usize] = *x;
            cnt[k] += 1;
        }
    }
    for i in 0..nsub {
        let s = &mut out[starts[i] as usize..starts[i + 1] as usize];
        if s.len() > 24 {
            s.sort_unstable_by_key(|x| big_key(x));
            if !fix {
                continue;
            }
            // exact order with the comparator (cheap on nearly sorted data)
        }
        for a in 1..s.len() {
            let x = s[a];
            let mut b = a;
            while b > 0 && less(&x, &s[b - 1]) {
                s[b] = s[b - 1];
                b -= 1;
            }
            s[b] = x;
        }
    }
    out
}

/// Gather the slices and order them by (16-bit person hash, person, ts, ...):
/// LSD radix sort on (hash16 << 32 | ts), then an exact insertion pass with
/// `less` (fixes persons sharing a hash and ties; near-linear on sorted data).
fn radix_gather<'a, T: Copy + 'a, I: Iterator<Item = &'a [T]> + Clone>(
    src: I,
    key: impl Fn(&T) -> u64,
    less: impl Fn(&T, &T) -> bool,
) -> Vec<T> {
    let n: usize = src.clone().map(|s| s.len()).sum();
    let mut a: Vec<T> = Vec::with_capacity(n);
    for sl in src {
        a.extend_from_slice(sl);
    }
    // sort packed words (32-bit key << 32 | index): 3 LSD passes of 11 bits
    let mut w: Vec<u64> = a.iter().enumerate().map(|(i, x)| (key(x) << 32) | i as u64).collect();
    let mut w2: Vec<u64> = Vec::with_capacity(n);
    unsafe { w2.set_len(n) };
    const D: u32 = 11;
    let mut shift = 32;
    while shift < 64 {
        let mut cnt = [0u32; 1 << D];
        for &k in w.iter() {
            cnt[((k >> shift) & ((1 << D) - 1)) as usize] += 1;
        }
        if cnt.iter().any(|&c| c as usize == n) {
            shift += D;
            continue;
        }
        let mut sum = 0u32;
        for c in cnt.iter_mut() {
            let t = *c;
            *c = sum;
            sum += t;
        }
        for &k in w.iter() {
            let d = ((k >> shift) & ((1 << D) - 1)) as usize;
            unsafe { *w2.get_unchecked_mut(cnt[d] as usize) = k };
            cnt[d] += 1;
        }
        std::mem::swap(&mut w, &mut w2);
        shift += D;
    }
    let mut out: Vec<T> = Vec::with_capacity(n);
    for &k in w.iter() {
        out.push(unsafe { *a.get_unchecked((k & 0xFFFF_FFFF) as usize) });
    }
    // exact fix-up
    for i in 1..n {
        if less(&out[i], &out[i - 1]) {
            let x = out[i];
            let mut j = i;
            while j > 0 && less(&x, &out[j - 1]) {
                out[j] = out[j - 1];
                j -= 1;
            }
            out[j] = x;
        }
    }
    out
}
#[inline]
fn h16(p: u64) -> u64 {
    (mix(p as u128) >> 8) & 0xFFFF
}
#[cfg(test)]
mod bench_tok {
    #[test]
    fn tok_speed() {
        let p = std::env::var("TOKFILE").unwrap_or_default();
        if p.is_empty() { return; }
        let d = std::fs::read(&p).unwrap();
        for _ in 0..2 {
            let t = std::time::Instant::now();
            let n = crate::par::count_seps(&d);
            eprintln!("masks+popcnt only {:.2} GB/s ({n})", d.len() as f64 / t.elapsed().as_secs_f64() / 1e9);
        }
        for _ in 0..2 {
            let t = std::time::Instant::now();
            let mut acc = 0u64;
            crate::par::each_record::<7>(&d, |[a, _b, _c, _d, _e, _f, _g]| { acc += a.len() as u64; });
            eprintln!("each_record {:.2} GB/s ({acc})", d.len() as f64 / t.elapsed().as_secs_f64() / 1e9);
        }
    }
}

#[cfg(test)]
mod tok_equiv {
    #[test]
    fn tokenizer_matches_reference() {
        let mut x: u64 = 0x2545_F491_4F6C_DD1D;
        let mut rnd = || { x ^= x << 13; x ^= x >> 7; x ^= x << 17; x };
        let alpha = b"ab,\"\n,,x\"1\n\"\"";
        for it in 0..40000 {
            let len = (rnd() % 300) as usize;
            let mut d: Vec<u8> = (0..len).map(|_| alpha[(rnd() % alpha.len() as u64) as usize]).collect();
            if it % 2 == 0 {
                d.clear();
                for _ in 0..(rnd() % 20) {
                    let fields: Vec<String> = (0..(5 + rnd() % 4)).map(|_| {
                        let v = if rnd() % 7 == 0 { String::new() } else { format!("v{}", rnd() % 100) };
                        match rnd() % 9 { 0 => format!("\"{v}\""), 1 => format!("\"{v}"), 2 => format!("{v}\""), 3 => format!("\"{v}\"\""), _ => v }
                    }).collect();
                    d.extend_from_slice(fields.join(",").as_bytes());
                    if rnd() % 10 != 0 { d.push(b'\n'); }
                }
            }
            for n in [7usize, 2, 4] {
                let mut a: Vec<Vec<Vec<u8>>> = vec![];
                let mut b: Vec<Vec<Vec<u8>>> = vec![];
                match n {
                    7 => { crate::par::each_record_old::<7>(&d, |fs| a.push(fs.iter().map(|x| x.to_vec()).collect())); crate::par::each_record::<7>(&d, |fs| b.push(fs.iter().map(|x| x.to_vec()).collect())); }
                    2 => { crate::par::each_record_old::<2>(&d, |fs| a.push(fs.iter().map(|x| x.to_vec()).collect())); crate::par::each_record::<2>(&d, |fs| b.push(fs.iter().map(|x| x.to_vec()).collect())); }
                    _ => { crate::par::each_record_old::<4>(&d, |fs| a.push(fs.iter().map(|x| x.to_vec()).collect())); crate::par::each_record::<4>(&d, |fs| b.push(fs.iter().map(|x| x.to_vec()).collect())); }
                }
                assert_eq!(a, b, "N={n} {:?}", String::from_utf8_lossy(&d));
            }
        }
    }
}
