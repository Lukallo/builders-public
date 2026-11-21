// Minimal parallel helpers on std threads, and chunked reading of CSV files.
use std::fs::File;
use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};

pub fn nthreads() -> usize {
    std::env::var("NCPU")
        .ok()
        .and_then(|s| s.trim().parse::<usize>().ok())
        .filter(|&n| n > 0)
        .unwrap_or_else(|| std::thread::available_parallelism().map(|n| n.get()).unwrap_or(4))
}

/// Run f(0..n) on a pool of threads (dynamic scheduling); results in index order.
pub fn par_map<T: Send, F: Fn(usize) -> T + Sync>(n: usize, f: F) -> Vec<T> {
    let next = AtomicUsize::new(0);
    let nt = nthreads().min(n.max(1));
    let mut parts: Vec<Vec<(usize, T)>> = std::thread::scope(|s| {
        let hs: Vec<_> = (0..nt)
            .map(|_| {
                s.spawn(|| {
                    let mut v = vec![];
                    loop {
                        let i = next.fetch_add(1, Ordering::Relaxed);
                        if i >= n {
                            break;
                        }
                        v.push((i, f(i)));
                    }
                    v
                })
            })
            .collect();
        hs.into_iter().map(|h| h.join().unwrap()).collect()
    });
    let mut slots: Vec<Option<T>> = (0..n).map(|_| None).collect();
    for p in parts.iter_mut() {
        for (i, t) in p.drain(..) {
            slots[i] = Some(t);
        }
    }
    slots.into_iter().map(|x| x.unwrap()).collect()
}

pub struct Task {
    file: usize,
    start: u64,
    end: u64,
}

/// The CSV files of one folder, split into byte ranges. A task owns the lines
/// that start inside its range; the first line of each file (header) is skipped.
pub struct Source {
    files: Vec<File>,
    sizes: Vec<u64>,
    pub tasks: Vec<Task>,
    maps: Vec<(*const u8, usize)>,
}
unsafe impl Sync for Source {}
unsafe impl Send for Source {}
unsafe extern "C" {
    fn mmap(addr: *mut u8, len: usize, prot: i32, flags: i32, fd: i32, off: i64) -> *mut u8;
    fn madvise(addr: *mut u8, len: usize, advice: i32) -> i32;
}

pub fn list_csv(dir: &Path) -> Vec<PathBuf> {
    let mut v: Vec<PathBuf> = match std::fs::read_dir(dir) {
        Ok(rd) => rd
            .filter_map(|e| e.ok())
            .map(|e| e.path())
            .filter(|p| {
                p.extension().map(|x| x == "csv").unwrap_or(false)
                    && std::fs::symlink_metadata(p).map(|m| !m.is_dir()).unwrap_or(false)
                    && p.is_file()
            })
            .collect(),
        Err(_) => vec![],
    };
    v.sort();
    v
}

impl Source {
    pub fn open(dir: &Path, target: u64) -> Source {
        let mut files = vec![];
        let mut sizes = vec![];
        let mut tasks = vec![];
        for p in list_csv(dir) {
            let Ok(f) = File::open(&p) else { continue };
            let size = f.metadata().map(|m| m.len()).unwrap_or(0);
            let fi = files.len();
            files.push(f);
            sizes.push(size);
            let mut s = 0;
            while s < size {
                let e = (s + target).min(size);
                tasks.push(Task { file: fi, start: s, end: e });
                s = e;
            }
        }
        // biggest-first is not needed: tasks are uniform in size
        let use_mmap = std::env::var_os("SS_PREAD").is_none();
        let maps = files
            .iter()
            .zip(sizes.iter())
            .map(|(f, &sz)| {
                if !use_mmap || sz == 0 {
                    return (std::ptr::null(), 0usize);
                }
                use std::os::unix::io::AsRawFd;
                // PROT_READ=1, MAP_PRIVATE=2, MAP_POPULATE=0x8000
                let p = unsafe { mmap(std::ptr::null_mut(), sz as usize, 1, 2, f.as_raw_fd(), 0) };
                if p as isize == -1 {
                    return (std::ptr::null(), 0usize);
                }
                unsafe { madvise(p, sz as usize, 2) }; // MADV_SEQUENTIAL
                (p as *const u8, sz as usize)
            })
            .collect();
        Source { files, sizes, tasks, maps }
    }

    /// Calls f once per task with a slice holding exactly the task's whole lines.
    pub fn run<T: Send, F: Fn(&[u8]) -> T + Sync>(&self, f: F) -> Vec<T> {
        par_map(self.tasks.len(), |ti| self.run_one(ti, &f))
    }

    /// Runs f on task ti's whole lines.
    pub fn run_one<T, F: Fn(&[u8]) -> T>(&self, ti: usize, f: &F) -> T {
        thread_local! {
            static BUF: std::cell::RefCell<Vec<u8>> = const { std::cell::RefCell::new(Vec::new()) };
        }
        {
            let t = &self.tasks[ti];
            let (mp, ml) = self.maps[t.file];
            if !mp.is_null() {
                let d = unsafe { std::slice::from_raw_parts(mp, ml) };
                let start = t.start as usize;
                let end = t.end as usize;
                // first line start
                let first = if start == 0 {
                    match d.iter().position(|&c| c == b'\n') { Some(p) => p + 1, None => ml }
                } else {
                    match d[start - 1..].iter().position(|&c| c == b'\n') { Some(p) => start - 1 + p + 1, None => ml }
                };
                if first >= end {
                    return f(&[]);
                }
                let stop = match d[end - 1..].iter().position(|&c| c == b'\n') { Some(p) => end - 1 + p + 1, None => ml };
                return f(&d[first..stop]);
            }
            BUF.with(|b| {
                let mut buf = b.borrow_mut();
                let t = &self.tasks[ti];
                let file = &self.files[t.file];
                let size = self.sizes[t.file];
                let from = if t.start == 0 { 0 } else { t.start - 1 };
                let mut want = (t.end + 65536).min(size);
                loop {
                    let len = (want - from) as usize;
                    if buf.capacity() < len {
                        let cur = buf.len();
                        buf.reserve(len - cur);
                    }
                    // SAFETY: bytes are fully written by read_full before use
                    unsafe { buf.set_len(len) };
                    read_full(file, &mut buf[..len], from);
                    // first line start
                    let first = if t.start == 0 {
                        // skip header
                        match buf.iter().position(|&c| c == b'\n') {
                            Some(p) => p + 1,
                            None => {
                                if want < size {
                                    want = (want * 2).min(size);
                                    continue;
                                }
                                len
                            }
                        }
                    } else {
                        match buf.iter().position(|&c| c == b'\n') {
                            Some(p) => p + 1,
                            None => {
                                if want < size {
                                    want = (want + (want - from)).min(size);
                                    continue;
                                }
                                len
                            }
                        }
                    };
                    // lines starting at absolute offset < t.end belong to us
                    let lim = (t.end - from) as usize; // relative
                    if first >= lim {
                        return f(&[]);
                    }
                    // find end: the newline terminating the line that contains lim-1
                    let end = match buf[lim - 1..].iter().position(|&c| c == b'\n') {
                        Some(p) => lim - 1 + p + 1,
                        None => {
                            if want < size {
                                want = (want + (want - from)).min(size);
                                continue;
                            }
                            len
                        }
                    };
                    return f(&buf[first..end]);
                }
            })
        }
    }
}

fn read_full(f: &File, buf: &mut [u8], off: u64) {
    let mut done = 0;
    while done < buf.len() {
        match f.read_at(&mut buf[done..], off + done as u64) {
            Ok(0) => {
                for x in &mut buf[done..] {
                    *x = 0;
                }
                break;
            }
            Ok(n) => done += n,
            Err(e) if e.kind() == std::io::ErrorKind::Interrupted => {}
            Err(e) => panic!("read: {e}"),
        }
    }
}

// ---------- SIMD record tokenizer ----------
#[inline(always)]
fn masks(d: &[u8], base: usize) -> (u64, u64, u64) {
    let n = d.len();
    if base + 64 <= n {
        masks64(&d[base..base + 64])
    } else {
        let mut tmp = [0u8; 64];
        tmp[..n - base].copy_from_slice(&d[base..]);
        masks64(&tmp)
    }
}

#[cfg(all(target_arch = "x86_64", target_feature = "avx512bw"))]
#[inline(always)]
fn masks64(b: &[u8]) -> (u64, u64, u64) {
    use std::arch::x86_64::*;
    unsafe {
        let v = _mm512_loadu_si512(b.as_ptr() as *const _);
        (
            _mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8(b',' as i8)),
            _mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8(b'\n' as i8)),
            _mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8(b'"' as i8)),
        )
    }
}
#[cfg(all(target_arch = "x86_64", target_feature = "avx2", not(target_feature = "avx512bw")))]
#[inline(always)]
fn masks64(b: &[u8]) -> (u64, u64, u64) {
    use std::arch::x86_64::*;
    unsafe {
        let lo = _mm256_loadu_si256(b.as_ptr() as *const _);
        let hi = _mm256_loadu_si256(b.as_ptr().add(32) as *const _);
        let m = |c: u8| {
            let cv = _mm256_set1_epi8(c as i8);
            let a = _mm256_movemask_epi8(_mm256_cmpeq_epi8(lo, cv)) as u32 as u64;
            let b = _mm256_movemask_epi8(_mm256_cmpeq_epi8(hi, cv)) as u32 as u64;
            a | (b << 32)
        };
        (m(b','), m(b'\n'), m(b'"'))
    }
}
#[cfg(not(all(target_arch = "x86_64", any(target_feature = "avx512bw", target_feature = "avx2"))))]
#[inline(always)]
fn masks64(b: &[u8]) -> (u64, u64, u64) {
    let (mut c, mut n, mut q) = (0u64, 0u64, 0u64);
    for (i, &x) in b.iter().enumerate() {
        c |= ((x == b',') as u64) << i;
        n |= ((x == b'\n') as u64) << i;
        q |= ((x == b'"') as u64) << i;
    }
    (c, n, q)
}

/// Reference tokenizer (kept for tests).
#[allow(dead_code)]
#[inline(always)]
pub fn each_record_old<const N: usize>(d: &[u8], mut f: impl FnMut([&[u8]; N])) {
    let n = d.len();
    let mut commas = [0usize; 16];
    let mut nc = 0usize;
    let mut ls = 0usize;
    let mut nq = 0u32;
    let mut base = 0usize;
    let mut finish = |ls: usize, end: usize, nc: usize, commas: &[usize; 16], nq: u32| {
        if nc != N - 1 {
            return;
        }
        let mut out: [&[u8]; N] = [&[]; N];
        let mut s = ls;
        for k in 0..N {
            let e = if k < N - 1 { commas[k] } else { end };
            out[k] = &d[s..e];
            s = e + 1;
        }
        if nq != 0 {
            // valid iff every quote is a matched pair around a whole field
            let mut paired = 0u32;
            for fld in out.iter_mut() {
                let l = fld.len();
                if l >= 2 && fld[0] == b'"' && fld[l - 1] == b'"' {
                    paired += 2;
                    *fld = &fld[1..l - 1];
                }
            }
            if paired != nq {
                return;
            }
        }
        f(out);
    };
    while base < n {
        let (mut mc, mut mn, mut mq) = masks(d, base);
        while mn != 0 {
            let b = mn.trailing_zeros();
            let below = (1u64 << b) - 1;
            let mut cm = mc & below;
            while cm != 0 {
                commas[nc & 15] = base + cm.trailing_zeros() as usize;
                nc += 1;
                cm &= cm - 1;
            }
            nq += (mq & below).count_ones();
            let p = base + b as usize;
            finish(ls, p, nc, &commas, nq);
            ls = p + 1;
            nc = 0;
            nq = 0;
            let keep = !(below | (1u64 << b));
            mc &= keep;
            mq &= keep;
            mn &= mn - 1;
        }
        while mc != 0 {
            commas[nc & 15] = base + mc.trailing_zeros() as usize;
            nc += 1;
            mc &= mc - 1;
        }
        nq += mq.count_ones();
        base += 64;
    }
    if ls < n {
        finish(ls, n, nc, &commas, nq);
    }
}

/// Like par_map, but each worker thread keeps one state built by `init`; tasks
/// are taken in the given `order`. Returns the per-thread states.
pub fn par_fold<S: Send, I: Fn() -> S + Sync, F: Fn(&mut S, usize) + Sync>(order: &[usize], init: I, f: F) -> Vec<S> {
    let next = AtomicUsize::new(0);
    let nt = nthreads().min(order.len().max(1));
    std::thread::scope(|s| {
        let hs: Vec<_> = (0..nt)
            .map(|_| {
                s.spawn(|| {
                    let mut st = init();
                    loop {
                        let i = next.fetch_add(1, Ordering::Relaxed);
                        if i >= order.len() {
                            break;
                        }
                        f(&mut st, order[i]);
                    }
                    st
                })
            })
            .collect();
        hs.into_iter().map(|h| h.join().unwrap()).collect()
    })
}

#[allow(dead_code)]
pub fn count_seps(d: &[u8]) -> u64 {
    let mut n = 0u64;
    let mut base = 0;
    while base < d.len() {
        let (c, l, q) = masks(d, base);
        n += (c | l).count_ones() as u64 + q.count_ones() as u64;
        base += 64;
    }
    n
}

/// Calls f with the N fields (quotes removed) of every well-formed line in d.
/// Quotes are counted per field from the bitmasks, so the byte checks run only
/// for fields that contain quotes.
#[inline(always)]
pub fn each_record<const N: usize>(d: &[u8], mut f: impl FnMut([&[u8]; N])) {
    let n = d.len();
    let mut seps = [0usize; 16];
    let mut qf = [0u32; 16];
    let mut nc = 0usize; // separators seen in the current line (commas)
    let mut anyq = 0u32;
    let mut cur_q = 0u32;
    let mut ls = 0usize;
    let mut base = 0usize;
    #[inline(always)]
    fn emit<const N: usize>(
        d: &[u8],
        ls: usize,
        end: usize,
        nc: usize,
        seps: &[usize; 16],
        qf: &[u32; 16],
        anyq: u32,
        f: &mut impl FnMut([&[u8]; N]),
    ) {
        if nc != N - 1 {
            return;
        }
        let mut out: [&[u8]; N] = [&[]; N];
        let mut s = ls;
        if anyq == 0 {
            for k in 0..N {
                let e = if k < N - 1 { seps[k] } else { end };
                out[k] = unsafe { d.get_unchecked(s..e) };
                s = e + 1;
            }
            f(out);
            return;
        }
        // branchless per-field quote validation and stripping
        let last_ix = d.len() - 1;
        let mut bad = false;
        for k in 0..N {
            let e = if k < N - 1 { seps[k] } else { end };
            let l = e - s;
            let q = qf[k];
            let first = unsafe { *d.get_unchecked(s.min(last_ix)) };
            let last = unsafe { *d.get_unchecked(e.saturating_sub(1).min(last_ix)) };
            let pair = (q == 2) & (l >= 2) & (first == b'"') & (last == b'"');
            bad |= (q != 0) & !pair;
            let adj = pair as usize;
            out[k] = unsafe { d.get_unchecked(s + adj..e - adj) };
            s = e + 1;
        }
        if !bad {
            f(out);
        }
    }
    while base < n {
        let (mc, mn, mut mq) = masks(d, base);
        let mut m = mc | mn;
        while m != 0 {
            let b = m.trailing_zeros();
            let below = (1u64 << b) - 1;
            let qb = (mq & below).count_ones();
            cur_q += qb;
            mq &= !below;
            let p = base + b as usize;
            if (mn >> b) & 1 == 1 {
                qf[nc & 15] = cur_q;
                anyq |= cur_q;
                emit::<N>(d, ls, p, nc, &seps, &qf, anyq, &mut f);
                ls = p + 1;
                nc = 0;
                anyq = 0;
            } else {
                seps[nc & 15] = p;
                qf[nc & 15] = cur_q;
                anyq |= cur_q;
                nc += 1;
            }
            cur_q = 0;
            m &= m - 1;
        }
        cur_q += mq.count_ones();
        base += 64;
    }
    if ls < n {
        qf[nc & 15] = cur_q;
        anyq |= cur_q;
        emit::<N>(d, ls, n, nc, &seps, &qf, anyq, &mut f);
    }
}
