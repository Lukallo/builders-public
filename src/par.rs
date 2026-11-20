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
        Source { files, sizes, tasks }
    }

    /// Calls f once per task with a slice holding exactly the task's whole lines.
    pub fn run<T: Send, F: Fn(&[u8]) -> T + Sync>(&self, f: F) -> Vec<T> {
        thread_local! {
            static BUF: std::cell::RefCell<Vec<u8>> = const { std::cell::RefCell::new(Vec::new()) };
        }
        par_map(self.tasks.len(), |ti| {
            BUF.with(|b| {
                let mut buf = b.borrow_mut();
                let t = &self.tasks[ti];
                let file = &self.files[t.file];
                let size = self.sizes[t.file];
                let from = if t.start == 0 { 0 } else { t.start - 1 };
                let mut want = (t.end + 65536).min(size);
                loop {
                    let len = (want - from) as usize;
                    buf.resize(len, 0);
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
        })
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
#[cfg(not(all(target_arch = "x86_64", target_feature = "avx512bw")))]
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

/// Calls f with the N fields (quotes removed) of every well-formed line in d.
#[inline(always)]
pub fn each_record<const N: usize>(d: &[u8], mut f: impl FnMut([&[u8]; N])) {
    let n = d.len();
    let mut commas = [0usize; 16];
    let mut nc = 0usize;
    let mut ls = 0usize;
    let mut quoted = false;
    let mut base = 0usize;
    let mut finish = |ls: usize, end: usize, nc: usize, commas: &[usize; 16], quoted: bool| {
        if nc != N - 1 {
            return;
        }
        let line = &d[ls..end];
        if quoted {
            if let Some(fs) = crate::split::<N>(line) {
                f(fs);
            }
            return;
        }
        let mut out: [&[u8]; N] = [&[]; N];
        let mut s = ls;
        for k in 0..N - 1 {
            out[k] = &d[s..commas[k]];
            s = commas[k] + 1;
        }
        out[N - 1] = &d[s..end];
        f(out);
    };
    while base < n {
        let (mc, mn, mq) = masks(d, base);
        let mut m = mc | mn;
        let mut qrem = mq;
        while m != 0 {
            let b = m.trailing_zeros() as usize;
            let below = qrem & ((1u64 << b) - 1);
            if below != 0 {
                quoted = true;
                qrem &= !below;
            }
            let p = base + b;
            if (mn >> b) & 1 == 1 {
                finish(ls, p, nc, &commas, quoted);
                ls = p + 1;
                nc = 0;
                quoted = false;
            } else {
                if nc < 16 {
                    commas[nc] = p;
                }
                nc += 1;
            }
            m &= m - 1;
        }
        if qrem != 0 {
            quoted = true;
        }
        base += 64;
    }
    if ls < n {
        finish(ls, n, nc, &commas, quoted);
    }
}
