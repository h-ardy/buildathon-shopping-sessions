// Shopping Sessions pipeline. Reads DATA_DIR, writes OUTPUT_DIR/user_daily and OUTPUT_DIR/campaign_daily,
// following rules.md exactly.

use memchr::{memchr, memchr3_iter};
use parquet::basic::Compression;
use parquet::data_type::{ByteArray, ByteArrayType, Int32Type, Int64Type};
use parquet::file::properties::{EnabledStatistics, WriterProperties};
use parquet::file::writer::SerializedFileWriter;
use parquet::schema::parser::parse_message_type;
use rayon::prelude::*;
use rustc_hash::{FxHashMap, FxHasher};
use std::hash::Hasher;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Instant;

const DAY: i64 = 86_400_000;
const WEEK: i64 = 7 * DAY;
const GAP: i64 = 1_800_000;
const BAD: i64 = i64::MIN;
const PB_BITS: u32 = 12;
const PB: usize = 1 << PB_BITS; // person buckets
const DB_BITS: u32 = 8;
const DB: usize = 1 << DB_BITS; // dedup buckets
fn chunk_size() -> u64 {
    static C: std::sync::OnceLock<u64> = std::sync::OnceLock::new();
    *C.get_or_init(|| std::env::var("SHOP_CHUNK").ok().and_then(|s| s.parse().ok()).unwrap_or(8 << 20))
}

type S = &'static [u8];

// Large buffers come straight from mmap with transparent huge pages requested: the pipeline touches
// several GB of fresh memory, and 4 KB page faults would otherwise cost seconds of kernel time.
struct HugeAlloc;

/// bit 0: ask for transparent huge pages; bit 1: prefault with MAP_POPULATE. Plain atomics only:
/// this runs inside the allocator, so it must not allocate.
static ALLOC_MODE: std::sync::atomic::AtomicI32 = std::sync::atomic::AtomicI32::new(-1);
fn alloc_mode() -> i32 {
    let m = ALLOC_MODE.load(std::sync::atomic::Ordering::Relaxed);
    if m >= 0 {
        return m;
    }
    let mut v = 1; // default: huge pages for big buffers
    let p = unsafe { libc::getenv(b"SHOP_ALLOC\0".as_ptr() as *const libc::c_char) };
    if !p.is_null() {
        let c = unsafe { *p } as u8;
        if c.is_ascii_digit() {
            v = (c - b'0') as i32;
        }
    }
    ALLOC_MODE.store(v, std::sync::atomic::Ordering::Relaxed);
    v
}
const BIG: usize = 4 << 20;

unsafe impl std::alloc::GlobalAlloc for HugeAlloc {
    unsafe fn alloc(&self, l: std::alloc::Layout) -> *mut u8 {
        if l.size() >= BIG && l.align() <= 4096 {
            let p = libc::mmap(
                std::ptr::null_mut(),
                l.size(),
                libc::PROT_READ | libc::PROT_WRITE,
                libc::MAP_PRIVATE | libc::MAP_ANONYMOUS | if alloc_mode() & 2 != 0 { libc::MAP_POPULATE } else { 0 },
                -1,
                0,
            );
            if p == libc::MAP_FAILED {
                return std::ptr::null_mut();
            }
            if alloc_mode() & 1 != 0 {
                libc::madvise(p, l.size(), libc::MADV_HUGEPAGE);
            }
            p as *mut u8
        } else {
            std::alloc::System.alloc(l)
        }
    }
    unsafe fn alloc_zeroed(&self, l: std::alloc::Layout) -> *mut u8 {
        if l.size() >= BIG && l.align() <= 4096 {
            self.alloc(l) // fresh anonymous mappings are already zero
        } else {
            std::alloc::System.alloc_zeroed(l)
        }
    }
    unsafe fn dealloc(&self, p: *mut u8, l: std::alloc::Layout) {
        if l.size() >= BIG && l.align() <= 4096 {
            if alloc_mode() & 4 == 0 {
                libc::munmap(p as *mut libc::c_void, l.size());
            } else {
                // keep the mapping but let the kernel drop its pages lazily, without a TLB shootdown now
                libc::madvise(p as *mut libc::c_void, l.size(), libc::MADV_FREE);
            }
        } else {
            std::alloc::System.dealloc(p, l)
        }
    }
    unsafe fn realloc(&self, p: *mut u8, l: std::alloc::Layout, new: usize) -> *mut u8 {
        let big_old = l.size() >= BIG && l.align() <= 4096;
        let big_new = new >= BIG && l.align() <= 4096;
        if !big_old && !big_new {
            return std::alloc::System.realloc(p, l, new);
        }
        if big_old && big_new {
            let q = libc::mremap(p as *mut libc::c_void, l.size(), new, libc::MREMAP_MAYMOVE);
            if q == libc::MAP_FAILED {
                return std::ptr::null_mut();
            }
            if alloc_mode() & 1 != 0 {
                libc::madvise(q, new, libc::MADV_HUGEPAGE);
            }
            return q as *mut u8;
        }
        let nl = std::alloc::Layout::from_size_align_unchecked(new, l.align());
        let q = self.alloc(nl);
        if !q.is_null() {
            std::ptr::copy_nonoverlapping(p, q, l.size().min(new));
            self.dealloc(p, l);
        }
        q
    }
}

#[global_allocator]
static GLOBAL: HugeAlloc = HugeAlloc;

fn cpu_secs() -> (f64, f64, i64) {
    let mut r: libc::rusage = unsafe { std::mem::zeroed() };
    unsafe { libc::getrusage(libc::RUSAGE_SELF, &mut r) };
    let tv = |t: libc::timeval| t.tv_sec as f64 + t.tv_usec as f64 / 1e6;
    (tv(r.ru_utime), tv(r.ru_stime), r.ru_minflt as i64)
}

macro_rules! log {
    ($t0:expr, $($arg:tt)*) => {{
        let (u, s, f) = cpu_secs();
        eprintln!("[{:7.3}s wall | cpu user {:6.2} sys {:5.2} faults {:8}] {}", $t0.elapsed().as_secs_f64(), u, s, f, format!($($arg)*))
    }};
}

/// Byte equality without a library call (names and IDs are short).
#[inline(always)]
fn eq_bytes(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    if a.as_ptr() == b.as_ptr() {
        return true;
    }
    let n = a.len();
    let mut i = 0;
    while i + 8 <= n {
        let x = unsafe { (a.as_ptr().add(i) as *const u64).read_unaligned() };
        let y = unsafe { (b.as_ptr().add(i) as *const u64).read_unaligned() };
        if x != y {
            return false;
        }
        i += 8;
    }
    while i < n {
        if a[i] != b[i] {
            return false;
        }
        i += 1;
    }
    true
}

#[inline]
fn hash_bytes(b: &[u8]) -> u64 {
    let mut h = FxHasher::default();
    h.write(b);
    h.write_usize(b.len());
    h.finish().wrapping_mul(0x9E37_79B9_7F4A_7C15)
}

// ---------------- CSV dialect ----------------

#[inline]
fn strip(f: S) -> Option<S> {
    if memchr(b'"', f).is_none() {
        return Some(f);
    }
    let n = f.len();
    if n >= 2 && f[0] == b'"' && f[n - 1] == b'"' && memchr(b'"', &f[1..n - 1]).is_none() {
        Some(&f[1..n - 1])
    } else {
        None
    }
}

/// Split a line into exactly N fields with quote rules applied. None if malformed.
#[inline]
fn split<const N: usize>(line: S) -> Option<[S; N]> {
    let mut out: [S; N] = [&[]; N];
    let mut k = 0;
    let mut start = 0;
    let mut rest = line;
    loop {
        match memchr(b',', rest) {
            Some(i) => {
                if k + 1 >= N {
                    return None;
                }
                out[k] = strip(&line[start..start + i])?;
                k += 1;
                start += i + 1;
                rest = &line[start..];
            }
            None => {
                if k + 1 != N {
                    return None;
                }
                out[k] = strip(&line[start..])?;
                return Some(out);
            }
        }
    }
}


/// Line/field state machine fed with the positions of every ',', '"' and '\n' in a chunk.
struct LineState<const N: usize> {
    out: [S; N],
    k: usize,  // fields completed in this line
    fs: usize, // current field start
    q: u32,    // quotes seen in current field
    q1: usize,
    q2: usize,
    bad: bool,
}

impl<const N: usize> LineState<N> {
    fn new() -> Self {
        LineState { out: [&[]; N], k: 0, fs: 0, q: 0, q1: 0, q2: 0, bad: false }
    }
    #[inline(always)]
    fn end_field(&mut self, buf: S, fe: usize) {
        if !self.bad {
            if self.k >= N {
                self.bad = true;
            } else if self.q == 0 {
                self.out[self.k] = &buf[self.fs..fe];
            } else if self.q == 2 && self.q1 == self.fs && self.q2 + 1 == fe && fe - self.fs >= 2 {
                self.out[self.k] = &buf[self.fs + 1..fe - 1];
            } else {
                self.bad = true;
            }
        }
        self.k += 1;
        self.q = 0;
    }
    #[inline(always)]
    fn on(&mut self, buf: S, p: usize, f: &mut impl FnMut(&[S; N])) {
        match buf[p] {
            b',' => {
                self.end_field(buf, p);
                self.fs = p + 1;
            }
            b'"' => {
                if self.q == 0 {
                    self.q1 = p;
                }
                self.q2 = p;
                self.q += 1;
            }
            _ => {
                self.end_field(buf, p);
                if !self.bad && self.k == N {
                    f(&self.out);
                }
                self.k = 0;
                self.bad = false;
                self.fs = p + 1;
            }
        }
    }
    #[inline(always)]
    fn finish(&mut self, buf: S, f: &mut impl FnMut(&[S; N])) {
        if self.fs < buf.len() {
            self.end_field(buf, buf.len());
            if !self.bad && self.k == N {
                f(&self.out);
            }
        }
    }
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2")]
unsafe fn scan_avx2<const N: usize>(buf: S, st: &mut LineState<N>, f: &mut impl FnMut(&[S; N])) {
    use std::arch::x86_64::*;
    let comma = _mm256_set1_epi8(b',' as i8);
    let quote = _mm256_set1_epi8(b'"' as i8);
    let nl = _mm256_set1_epi8(b'\n' as i8);
    let n = buf.len();
    let mut i = 0;
    while i + 32 <= n {
        let v = _mm256_loadu_si256(buf.as_ptr().add(i) as *const __m256i);
        let m = _mm256_or_si256(
            _mm256_or_si256(_mm256_cmpeq_epi8(v, comma), _mm256_cmpeq_epi8(v, quote)),
            _mm256_cmpeq_epi8(v, nl),
        );
        let mut mask = _mm256_movemask_epi8(m) as u32;
        while mask != 0 {
            let p = i + mask.trailing_zeros() as usize;
            st.on(buf, p, f);
            mask &= mask - 1;
        }
        i += 32;
    }
    while i < n {
        let c = *buf.get_unchecked(i);
        if c == b',' || c == b'"' || c == b'\n' {
            st.on(buf, i, f);
        }
        i += 1;
    }
}

/// Parse every line of a chunk into exactly N fields (quote rules applied); malformed lines are skipped.
#[inline]
fn parse_lines<const N: usize>(buf: S, mut f: impl FnMut(&[S; N])) {
    let mut st = LineState::<N>::new();
    #[cfg(target_arch = "x86_64")]
    {
        if std::is_x86_feature_detected!("avx2") && std::env::var_os("SHOP_NOSIMD").is_none() {
            unsafe { scan_avx2(buf, &mut st, &mut f) };
            st.finish(buf, &mut f);
            return;
        }
    }
    for p in memchr3_iter(b',', b'\n', b'"', buf) {
        st.on(buf, p, &mut f);
    }
    st.finish(buf, &mut f);
}

/// Iterate the lines of a chunk (only \n ends a line).
#[inline]
fn for_lines(mut buf: S, mut f: impl FnMut(S)) {
    while !buf.is_empty() {
        match memchr(b'\n', buf) {
            Some(i) => {
                f(&buf[..i]);
                buf = &buf[i + 1..];
            }
            None => {
                f(buf);
                break;
            }
        }
    }
}

// ---------------- timestamps ----------------

/// F3: ASCII digits only (leading zeros allowed) -> the number, BAD if not digits or over i64.
#[inline]
fn parse_f3(s: &[u8]) -> i64 {
    let mut i = 0;
    while i < s.len() && s[i] == b'0' {
        i += 1;
    }
    let t = &s[i..];
    if t.len() > 19 {
        return BAD; // either not all digits or too large: bad either way
    }
    let mut v: u64 = 0;
    let mut j = 0;
    let head = t.len() % 8;
    while j < head {
        let d = t[j].wrapping_sub(b'0');
        if d >= 10 {
            return BAD;
        }
        v = v * 10 + d as u64;
        j += 1;
    }
    while j < t.len() {
        let w = u64::from_le_bytes(t[j..j + 8].try_into().unwrap());
        if (w.wrapping_sub(0x3030_3030_3030_3030) | w.wrapping_add(0x4646_4646_4646_4646)) & 0x8080_8080_8080_8080 != 0 {
            return BAD;
        }
        let mut x = w.wrapping_sub(0x3030_3030_3030_3030);
        x = x.wrapping_mul(10).wrapping_add(x >> 8);
        x = ((x & 0x0000_00FF_0000_00FF).wrapping_mul(100 + (1_000_000 << 32))
            .wrapping_add(((x >> 16) & 0x0000_00FF_0000_00FF).wrapping_mul(1 + (10_000 << 32))))
            >> 32;
        v = match v.checked_mul(100_000_000).and_then(|v| v.checked_add(x)) {
            Some(v) => v,
            None => return BAD,
        };
        j += 8;
    }
    if s.is_empty() || v > i64::MAX as u64 {
        BAD
    } else {
        v as i64
    }
}

/// Days since 1970-01-01 of the first day of each month, for years 1900..2200 (index (y-1900)*12 + m-1),
/// plus one final entry. Built at compile time.
const MS_Y0: i64 = 1900;
const MS_NY: usize = 300;
const fn civil_days(y: i64, m: i64, d: i64) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = if y >= 0 { y } else { y - 399 } / 400;
    let yoe = y - era * 400;
    let mp = (m + 9) % 12;
    let doy = (153 * mp + 2) / 5 + d - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146_097 + doe - 719_468
}
const MONTH_START: [i32; MS_NY * 12 + 1] = {
    let mut t = [0i32; MS_NY * 12 + 1];
    let mut i = 0;
    while i <= MS_NY * 12 {
        let y = MS_Y0 + (i / 12) as i64;
        let m = (i % 12) as i64 + 1;
        t[i] = civil_days(y, m, 1) as i32;
        i += 1;
    }
    t
};

#[inline(always)]
fn month_info(y: i64, mo: i64) -> (i64, i64) {
    // (days since epoch of day 1 of the month, days in the month)
    let yi = y - MS_Y0;
    if (0..MS_NY as i64).contains(&yi) {
        let k = (yi * 12 + mo - 1) as usize;
        let s = MONTH_START[k] as i64;
        (s, MONTH_START[k + 1] as i64 - s)
    } else {
        let leap = y % 4 == 0 && (y % 100 != 0 || y % 400 == 0);
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
        (civil_days(y, mo, 1), dim)
    }
}

/// F1 (`YYYY-MM-DDTHH:MM:SSZ`) or F2 (`...±HH:MM`); the caller checked the length is 20 or 25.
#[inline]
fn parse_iso(s: &[u8]) -> i64 {
    let n = s.len();
    let g = |i: usize| s[i].wrapping_sub(b'0') as i64;
    let (y0, y1, y2, y3) = (g(0), g(1), g(2), g(3));
    let (m0, m1, d0, d1) = (g(5), g(6), g(8), g(9));
    let (h0, h1, i0, i1, s0, s1) = (g(11), g(12), g(14), g(15), g(17), g(18));
    let bad_digit = (y0 > 9) | (y1 > 9) | (y2 > 9) | (y3 > 9) | (m0 > 9) | (m1 > 9) | (d0 > 9) | (d1 > 9)
        | (h0 > 9) | (h1 > 9) | (i0 > 9) | (i1 > 9) | (s0 > 9) | (s1 > 9);
    let bad_sep = (s[4] != b'-') | (s[7] != b'-') | (s[10] != b'T') | (s[13] != b':') | (s[16] != b':');
    let y = y0 * 1000 + y1 * 100 + y2 * 10 + y3;
    let mo = m0 * 10 + m1;
    let d = d0 * 10 + d1;
    let h = h0 * 10 + h1;
    let mi = i0 * 10 + i1;
    let se = s0 * 10 + s1;
    if bad_digit | bad_sep | (mo < 1) | (mo > 12) | (d < 1) | (h > 23) | (mi > 59) | (se > 59) {
        return BAD;
    }
    let (ms, dim) = month_info(y, mo);
    if d > dim {
        return BAD;
    }
    let off = if n == 20 {
        if s[19] != b'Z' {
            return BAD;
        }
        0
    } else {
        let (a, b, c, e) = (g(20), g(21), g(23), g(24));
        let sign = match s[19] {
            b'+' => 1,
            b'-' => -1,
            _ => return BAD,
        };
        if (s[22] != b':') | (a > 9) | (b > 9) | (c > 9) | (e > 9) {
            return BAD;
        }
        let (oh, om) = (a * 10 + b, c * 10 + e);
        if oh > 23 || om > 59 {
            return BAD;
        }
        sign * (oh * 3_600_000 + om * 60_000)
    };
    (ms + d - 1) * DAY + h * 3_600_000 + mi * 60_000 + se * 1000 - off
}

#[inline]
fn parse_ts(s: &[u8]) -> i64 {
    let n = s.len();
    if (n == 20 || n == 25) && s[4] == b'-' {
        parse_iso(s)
    } else {
        parse_f3(s)
    }
}

/// Purchase price: ASCII digits only, value 1..=10,000,000. -1 if invalid.
#[inline]
fn parse_price(s: &[u8]) -> i32 {
    if s.is_empty() || !s.iter().all(|c| c.is_ascii_digit()) {
        return -1;
    }
    let mut i = 0;
    while i < s.len() && s[i] == b'0' {
        i += 1;
    }
    let t = &s[i..];
    if t.is_empty() || t.len() > 8 {
        return -1;
    }
    let mut v: i64 = 0;
    for &c in t {
        v = v * 10 + (c - b'0') as i64;
    }
    if (1..=10_000_000).contains(&v) {
        v as i32
    } else {
        -1
    }
}

// ---------------- files ----------------

fn csv_files(dir: &Path) -> Vec<PathBuf> {
    let mut v: Vec<PathBuf> = match std::fs::read_dir(dir) {
        Ok(rd) => rd
            .filter_map(|e| e.ok())
            .filter(|e| {
                let name = e.file_name();
                let n = name.to_string_lossy();
                n.ends_with(".csv") && !n.starts_with('.') && e.path().is_file()
            })
            .map(|e| e.path())
            .collect(),
        Err(_) => vec![],
    };
    v.sort();
    v
}

struct Task {
    file: usize,
    off: usize,
    end: usize,
}

struct Mapped {
    files: Vec<S>,
    tasks: Vec<Task>,
}

/// Map every file read-only (no copy) and cut it into slices of about chunk_size() bytes.
fn map_files(dir: &Path) -> Mapped {
    let paths = csv_files(dir);
    let files: Vec<S> = paths
        .par_iter()
        .map(|p| {
            let f = std::fs::File::open(p).unwrap_or_else(|e| panic!("open {}: {}", p.display(), e));
            let len = f.metadata().map(|m| m.len()).unwrap_or(0) as usize;
            if len == 0 {
                return &[][..];
            }
            use std::os::unix::io::AsRawFd;
            let ptr = unsafe { libc::mmap(std::ptr::null_mut(), len, libc::PROT_READ, libc::MAP_PRIVATE, f.as_raw_fd(), 0) };
            if ptr == libc::MAP_FAILED {
                // fall back to reading the file
                let v = std::fs::read(p).unwrap_or_else(|e| panic!("read {}: {}", p.display(), e));
                return &*Box::leak(v.into_boxed_slice());
            }
            unsafe { std::slice::from_raw_parts(ptr as *const u8, len) }
        })
        .collect();
    let cs = chunk_size() as usize;
    let mut tasks = Vec::new();
    for (i, b) in files.iter().enumerate() {
        let mut off = 0;
        while off < b.len() {
            let end = (off + cs).min(b.len());
            tasks.push(Task { file: i, off, end });
            off = end;
        }
    }
    Mapped { files, tasks }
}

/// The lines owned by this task: those whose first byte lies in [off, end). The header (first line of
/// the file) is skipped. Only \n ends a line.
fn task_lines(m: &Mapped, t: &Task) -> S {
    let b = m.files[t.file];
    let start = if t.off == 0 {
        memchr(b'\n', b).map(|i| i + 1).unwrap_or(b.len())
    } else if b[t.off - 1] == b'\n' {
        t.off
    } else {
        memchr(b'\n', &b[t.off..]).map(|i| t.off + i + 1).unwrap_or(b.len())
    };
    let end = if t.end >= b.len() || b[t.end - 1] == b'\n' {
        t.end
    } else {
        memchr(b'\n', &b[t.end..]).map(|i| t.end + i + 1).unwrap_or(b.len())
    };
    if start >= end {
        return &[];
    }
    let s = &b[start..end];
    #[cfg(target_os = "linux")]
    unsafe {
        // fault the slice in with one call instead of one page fault per 4 KB (Linux 5.14+; ignored if not)
        let pg = 4096usize;
        let a = (s.as_ptr() as usize) & !(pg - 1);
        let len = s.as_ptr() as usize + s.len() - a;
        libc::madvise(a as *mut libc::c_void, len, 22 /* MADV_POPULATE_READ */);
    }
    s
}

fn read_manifest(data: &Path) -> (i64, i64) {
    let s = std::fs::read_to_string(data.join("manifest.json")).expect("manifest.json");
    let get = |key: &str| -> i64 {
        let k = s.find(&format!("\"{}\"", key)).expect("manifest key");
        let after = &s[k + key.len() + 2..];
        let c = after.find(':').unwrap();
        let q1 = after[c..].find('"').unwrap() + c + 1;
        let q2 = after[q1..].find('"').unwrap() + q1;
        let t = parse_ts(after[q1..q2].as_bytes());
        assert!(t != BAD, "bad manifest time");
        t
    };
    (get("window_start"), get("window_end"))
}

// ---------------- scatter (counting sort by bucket) ----------------

/// Bucket every part by key in place (through a reused scratch buffer, so no fresh memory per part).
/// Returns, per part, the start of each bucket (nb + 1 entries). Order inside a bucket is kept.
fn bucket_parts<T: Copy + Send + Sync, F: Fn(&T) -> usize + Sync>(parts: &mut [Vec<T>], nb: usize, key: F) -> Vec<Vec<u32>> {
    parts
        .par_iter_mut()
        .map_init(Vec::<T>::new, |scratch, v| {
            let mut cnt = vec![0u32; nb + 1];
            for x in v.iter() {
                cnt[key(x) + 1] += 1;
            }
            for b in 0..nb {
                cnt[b + 1] += cnt[b];
            }
            let mut w = cnt.clone();
            scratch.clear();
            scratch.reserve(v.len());
            let dst = scratch.as_mut_ptr();
            for x in v.iter() {
                let b = key(x);
                unsafe { dst.add(w[b] as usize).write(*x) };
                w[b] += 1;
            }
            unsafe { scratch.set_len(v.len()) };
            std::mem::swap(v, scratch);
            cnt
        })
        .collect()
}


// ---------------- records ----------------

/// A user or person ID kept inline when it is at most 15 bytes (zero padded, length in the last byte),
/// otherwise a pointer and length (last byte 0xFF). Comparing two short names never touches the input.
#[derive(Clone, Copy)]
#[repr(C, align(8))]
struct Name {
    w: [u8; 16],
}

impl Name {
    #[inline(always)]
    fn new(s: S) -> Name {
        let mut w = [0u8; 16];
        let n = s.len();
        if n <= 15 {
            unsafe { std::ptr::copy_nonoverlapping(s.as_ptr(), w.as_mut_ptr(), n) };
            w[15] = n as u8;
        } else {
            w[..8].copy_from_slice(&(s.as_ptr() as usize as u64).to_le_bytes());
            w[8..12].copy_from_slice(&(n as u32).to_le_bytes());
            w[15] = 0xFF;
        }
        Name { w }
    }
    #[inline(always)]
    fn bytes(&self) -> &[u8] {
        let t = self.w[15];
        if t != 0xFF {
            &self.w[..t as usize]
        } else {
            let p = u64::from_le_bytes(self.w[..8].try_into().unwrap()) as usize;
            let n = u32::from_le_bytes(self.w[8..12].try_into().unwrap()) as usize;
            unsafe { std::slice::from_raw_parts(p as *const u8, n) }
        }
    }
    #[inline(always)]
    fn same(&self, o: &Name) -> bool {
        if u128::from_ne_bytes(self.w) == u128::from_ne_bytes(o.w) {
            return true;
        }
        self.w[15] == 0xFF && o.w[15] == 0xFF && eq_bytes(self.bytes(), o.bytes())
    }
}

#[derive(Clone, Copy)]
struct RawEv {
    eh: u64,
    eid: S,
    uid: S,
    uh: u64,
    its: i64,
    ts: i64,
    price: i32,
    cat: u16,
    kind: u8, // 0 view, 1 cart, 2 purchase, 3 other; 4 = product empty (invalid)
}

#[derive(Clone, Copy)]
struct Ev {
    ph: u64,
    name: Name,
    ts: i64,
    price: i32,
    cat: u16,
    purchase: bool,
}

#[derive(Clone, Copy)]
struct Click {
    ph: u64,
    name: Name,
    ts: i64,
    cid: S,
    camp: u16,
}

/// One slot of the person table. All-zero bytes mean an empty slot (id_len == 0; ids are never empty).
#[repr(C, align(64))]
#[derive(Clone, Copy)]
struct PEntry {
    uh: u64,
    ph: u64,
    name: Name,
    id_len: u32,
    id_ptr: usize,
    key: [u8; 16], // first bytes of id, so most lookups never leave this cache line
}

impl PEntry {
    #[inline(always)]
    fn id(&self) -> S {
        unsafe { std::slice::from_raw_parts(self.id_ptr as *const u8, self.id_len as usize) }
    }
}

/// Open-addressing table: linked user ID -> person (name and its hash). IDs not in it are their own person.
struct People {
    tab: Vec<PEntry>,
    shift: u32,
    n: usize,
}

impl People {
    #[inline]
    fn resolve(&self, uh: u64, uid: &[u8]) -> Option<(u64, Name)> {
        if self.n > 0 {
            let mask = self.tab.len() - 1;
            let mut i = (uh >> self.shift) as usize;
            loop {
                let e = &self.tab[i];
                if e.id_len == 0 {
                    break;
                }
                if e.uh == uh && e.id_len as usize == uid.len() {
                    let n = uid.len();
                    let same = if n <= 16 { eq_bytes(&e.key[..n], uid) } else { eq_bytes(e.id(), uid) };
                    if same {
                        return Some((e.ph, e.name));
                    }
                }
                i = (i + 1) & mask;
            }
        }
        None
    }

    #[inline(always)]
    fn prefetch(&self, uh: u64) {
        #[cfg(target_arch = "x86_64")]
        if self.n > 0 {
            let i = (uh >> self.shift) as usize;
            unsafe {
                std::arch::x86_64::_mm_prefetch(self.tab.as_ptr().add(i) as *const i8, std::arch::x86_64::_MM_HINT_T0)
            };
        }
    }

    /// Resolve in place: on entry each item's (hash, name) holds the user ID's (hash, bytes).
    /// Table slots are prefetched a few items ahead so the cache misses overlap.
    fn resolve_all<T>(&self, v: &mut [T], get: impl Fn(&T) -> (u64, &[u8]), set: impl Fn(&mut T, u64, Name)) {
        const AHEAD: usize = 16;
        let n = v.len();
        for i in 0..n.min(AHEAD) {
            self.prefetch(get(&v[i]).0);
        }
        for i in 0..n {
            if i + AHEAD < n {
                self.prefetch(get(&v[i + AHEAD]).0);
            }
            let r = {
                let (uh, uid) = get(&v[i]);
                self.resolve(uh, uid)
            };
            if let Some((ph, name)) = r {
                set(&mut v[i], ph, name);
            }
        }
    }
}

/// Open-addressing map from short byte strings to a small value, compared without library calls.
struct StrMap<V: Copy> {
    tab: Vec<(u64, S, V)>,
    shift: u32,
    empty: V,
}

impl<V: Copy + PartialEq> StrMap<V> {
    fn new(items: impl IntoIterator<Item = (S, V)>, empty: V) -> Self {
        let items: Vec<(S, V)> = items.into_iter().collect();
        let cap = (items.len() * 2).next_power_of_two().max(2);
        let shift = 64 - cap.trailing_zeros();
        let mut tab = vec![(0u64, &[][..], empty); cap];
        let mut used = vec![false; cap];
        for (k, v) in items {
            let h = hash_bytes(k);
            let mut i = (h >> shift) as usize;
            loop {
                if !used[i] {
                    used[i] = true;
                    tab[i] = (h, k, v);
                    break;
                }
                if tab[i].0 == h && eq_bytes(tab[i].1, k) {
                    tab[i].2 = v; // duplicate key: last one wins
                    break;
                }
                i = (i + 1) & (cap - 1);
            }
        }
        StrMap { tab, shift, empty }
    }
    #[inline]
    fn get(&self, k: &[u8]) -> Option<V> {
        let h = hash_bytes(k);
        let mask = self.tab.len() - 1;
        let mut i = (h >> self.shift) as usize;
        loop {
            let e = &self.tab[i];
            if e.2 == self.empty {
                return None;
            }
            if e.0 == h && eq_bytes(e.1, k) {
                return Some(e.2);
            }
            i = (i + 1) & mask;
        }
    }
}

#[inline]
fn pbucket(ph: u64) -> usize {
    (ph >> (64 - PB_BITS)) as usize
}

fn find(parent: &mut [u32], mut x: u32) -> u32 {
    while parent[x as usize] != x {
        let p = parent[x as usize];
        parent[x as usize] = parent[p as usize];
        x = p;
    }
    x
}

fn build_people(data: &Path) -> People {
    const SH_BITS: u32 = 6;
    const NSH: usize = 1 << SH_BITS;
    let mp = map_files(&data.join("links"));
    // parse; send each id (with its hash) to a shard by hash
    let parts: Vec<(Vec<(u64, u64)>, Vec<Vec<(u64, S)>>)> = mp
        .tasks
        .par_iter()
        .map(|t| {
            let c = task_lines(&mp, t);
            let mut edges = Vec::new();
            let mut sh: Vec<Vec<(u64, S)>> = (0..NSH).map(|_| Vec::new()).collect();
            parse_lines::<2>(c, |&[a, b]| {
                if !a.is_empty() && !b.is_empty() {
                    let (ha, hb) = (hash_bytes(a), hash_bytes(b));
                    sh[(ha >> (64 - SH_BITS)) as usize].push((ha, a));
                    sh[(hb >> (64 - SH_BITS)) as usize].push((hb, b));
                    edges.push((ha, hb));
                }
            });
            (edges, sh)
        })
        .collect();
    let parts = &parts;
    // intern per shard in parallel: distinct ids of the shard, and a hash -> local index table
    struct Shard {
        ids: Vec<S>,
        hashes: Vec<u64>,
        map: FxHashMap<u64, u32>,
        coll: bool,
    }
    let shards: Vec<Shard> = (0..NSH)
        .into_par_iter()
        .map(|k| {
            let mut ids = Vec::new();
            let mut hashes = Vec::new();
            let mut map: FxHashMap<u64, u32> = FxHashMap::default();
            let mut coll = false;
            for (_, sh) in parts.iter() {
                for &(h, s) in &sh[k] {
                    match map.get(&h) {
                        Some(&i) => {
                            if !eq_bytes(ids[i as usize], s) {
                                coll = true;
                            }
                        }
                        None => {
                            map.insert(h, ids.len() as u32);
                            ids.push(s);
                            hashes.push(h);
                        }
                    }
                }
            }
            Shard { ids, hashes, map, coll }
        })
        .collect();
    if shards.iter().any(|s| s.coll) || std::env::var_os("SHOP_EXACT").is_some() {
        return build_people_exact(data);
    }
    let mut base = vec![0u32; NSH + 1];
    for k in 0..NSH {
        base[k + 1] = base[k] + shards[k].ids.len() as u32;
    }
    let n = base[NSH] as usize;
    let gid = |h: u64| -> u32 {
        let k = (h >> (64 - SH_BITS)) as usize;
        base[k] + shards[k].map[&h]
    };
    // edges as global ids (parallel lookups), then union-find
    let edges: Vec<Vec<(u32, u32)>> = parts.par_iter().map(|(e, _)| e.iter().map(|&(a, b)| (gid(a), gid(b))).collect()).collect();
    let mut parent: Vec<u32> = (0..n as u32).collect();
    for v in &edges {
        for &(a, b) in v {
            let (ra, rb) = (find(&mut parent, a), find(&mut parent, b));
            if ra != rb {
                parent[ra as usize] = rb;
            }
        }
    }
    let names: Vec<S> = shards.iter().flat_map(|s| s.ids.iter().copied()).collect();
    let uhs: Vec<u64> = shards.iter().flat_map(|s| s.hashes.iter().copied()).collect();
    drop(shards);
    let roots: Vec<u32> = (0..n as u32).map(|i| find(&mut parent, i)).collect();
    let mut best: Vec<u32> = (0..n as u32).collect();
    for i in 0..n {
        let r = roots[i] as usize;
        if names[i] < names[best[r] as usize] {
            best[r] = i as u32;
        }
    }
    people_table(&names, &uhs, &roots, &best)
}

fn people_table(names: &[S], uhs: &[u64], roots: &[u32], best: &[u32]) -> People {
    let n = names.len();
    let entries: Vec<PEntry> = (0..n)
        .into_par_iter()
        .map(|i| {
            let name = names[best[roots[i] as usize] as usize];
            let mut key = [0u8; 16];
            let k = names[i].len().min(16);
            key[..k].copy_from_slice(&names[i][..k]);
            PEntry {
                uh: uhs[i],
                ph: hash_bytes(name),
                name: Name::new(name),
                id_len: names[i].len() as u32,
                id_ptr: names[i].as_ptr() as usize,
                key,
            }
        })
        .collect();
    // at most about 3/8 full: an unlinked id (most lookups miss) then ends its probe within a line or two
    let mut cap = n.next_power_of_two().max(2);
    if n * 4 > cap * 3 {
        cap *= 2;
    }
    cap *= 2;
    let bits = cap.trailing_zeros();
    let shift = 64 - bits;
    let mut entries = entries;
    // place in home-slot order: linear probing then writes almost sequentially instead of at random
    entries.par_sort_unstable_by_key(|e| e.uh >> shift);
    // zeroed allocation: fresh pages, no pass writing empty slots
    let mut tab: Vec<PEntry> = unsafe {
        let layout = std::alloc::Layout::array::<PEntry>(cap).unwrap();
        let p = std::alloc::alloc_zeroed(layout) as *mut PEntry;
        assert!(!p.is_null(), "out of memory");
        Vec::from_raw_parts(p, cap, cap)
    };
    let mut next = 0usize; // first slot not yet known to be taken by the sorted run
    let mut wrapped = Vec::new();
    for e in entries {
        let home = (e.uh >> shift) as usize;
        let i = home.max(next);
        if i < cap {
            tab[i] = e;
            next = i + 1;
        } else {
            wrapped.push(e);
        }
    }
    for e in wrapped {
        let mut i = (e.uh >> shift) as usize & (cap - 1);
        while tab[i].id_len != 0 {
            i = (i + 1) & (cap - 1);
        }
        tab[i] = e;
    }
    People { tab, shift, n }
}

/// Slow, plainly exact version (used only if two different ids share a 64-bit hash).
fn build_people_exact(data: &Path) -> People {
    let mp = map_files(&data.join("links"));
    let mut idx: FxHashMap<S, u32> = FxHashMap::default();
    let mut names: Vec<S> = Vec::new();
    let mut parent: Vec<u32> = Vec::new();
    for t in &mp.tasks {
        parse_lines::<2>(task_lines(&mp, t), |&[a, b]| {
            if a.is_empty() || b.is_empty() {
                return;
            }
            let mut ids = [0u32; 2];
            for (k, s) in [a, b].into_iter().enumerate() {
                ids[k] = *idx.entry(s).or_insert_with(|| {
                    let i = names.len() as u32;
                    names.push(s);
                    parent.push(i);
                    i
                });
            }
            let (ra, rb) = (find(&mut parent, ids[0]), find(&mut parent, ids[1]));
            if ra != rb {
                parent[ra as usize] = rb;
            }
        });
    }
    let n = names.len();
    let roots: Vec<u32> = (0..n as u32).map(|i| find(&mut parent, i)).collect();
    let mut best: Vec<u32> = (0..n as u32).collect();
    for i in 0..n {
        let r = roots[i] as usize;
        if names[i] < names[best[r] as usize] {
            best[r] = i as u32;
        }
    }
    let uhs: Vec<u64> = names.iter().map(|s| hash_bytes(s)).collect();
    people_table(&names, &uhs, &roots, &best)
}

fn read_ref<const N: usize>(path: &Path) -> Vec<[S; N]> {
    let b = std::fs::read(path).unwrap_or_else(|e| panic!("read {}: {}", path.display(), e));
    let b: S = Box::leak(b.into_boxed_slice());
    let mut out = Vec::new();
    if let Some(i) = memchr(b'\n', b) {
        for_lines(&b[i + 1..], |l| {
            let l = l.strip_suffix(b"\r").unwrap_or(l);
            if let Some(r) = split::<N>(l) {
                out.push(r);
            }
        });
    }
    out
}

// ---------------- outputs ----------------

struct UserRow {
    name: Name,
    day: i32,
    sessions: i64,
    events: i64,
    rev: i64,
    top: u16, // u16::MAX = null
}

fn props() -> Arc<WriterProperties> {
    Arc::new(
        WriterProperties::builder()
            .set_compression(Compression::UNCOMPRESSED)
            .set_dictionary_enabled(false)
            .set_statistics_enabled(EnabledStatistics::None)
            .build(),
    )
}

fn write_user(path: &Path, rows: &[&UserRow], cats: &[Vec<u8>]) {
    let schema = Arc::new(
        parse_message_type(
            "message user_daily { required binary user_id (STRING); required int32 day (DATE);
             required int64 sessions; required int64 events; required int64 revenue_cents;
             optional binary top_category (STRING); }",
        )
        .unwrap(),
    );
    let file = std::fs::File::create(path).expect("create output");
    let mut w = SerializedFileWriter::new(std::io::BufWriter::with_capacity(1 << 20, file), schema, props()).unwrap();
    if !rows.is_empty() {
        let mut rg = w.next_row_group().unwrap();
        let mut col = 0;
        while let Some(mut c) = rg.next_column().unwrap() {
            match col {
                0 => {
                    // one shared buffer for all ids: no allocation per row
                    let total: usize = rows.iter().map(|r| r.name.bytes().len()).sum();
                    let mut buf = Vec::with_capacity(total);
                    for r in rows {
                        buf.extend_from_slice(r.name.bytes());
                    }
                    let buf = bytes::Bytes::from(buf);
                    let mut off = 0;
                    let v: Vec<ByteArray> = rows
                        .iter()
                        .map(|r| {
                            let b = buf.slice(off..off + r.name.bytes().len());
                            off += r.name.bytes().len();
                            ByteArray::from(b)
                        })
                        .collect();
                    c.typed::<ByteArrayType>().write_batch(&v, None, None).unwrap();
                }
                1 => {
                    let v: Vec<i32> = rows.iter().map(|r| r.day).collect();
                    c.typed::<Int32Type>().write_batch(&v, None, None).unwrap();
                }
                2 | 3 | 4 => {
                    let v: Vec<i64> = rows
                        .iter()
                        .map(|r| match col {
                            2 => r.sessions,
                            3 => r.events,
                            _ => r.rev,
                        })
                        .collect();
                    c.typed::<Int64Type>().write_batch(&v, None, None).unwrap();
                }
                _ => {
                    let cb: Vec<bytes::Bytes> = cats.iter().map(|c| bytes::Bytes::copy_from_slice(c)).collect();
                    let def: Vec<i16> = rows.iter().map(|r| (r.top != u16::MAX) as i16).collect();
                    let v: Vec<ByteArray> = rows
                        .iter()
                        .filter(|r| r.top != u16::MAX)
                        .map(|r| ByteArray::from(cb[r.top as usize].clone()))
                        .collect();
                    c.typed::<ByteArrayType>().write_batch(&v, Some(&def), None).unwrap();
                }
            }
            c.close().unwrap();
            col += 1;
        }
        rg.close().unwrap();
    }
    w.close().unwrap();
}

#[allow(clippy::too_many_arguments)]
fn write_campaign(path: &Path, ids: &[Vec<u8>], chans: &[Vec<u8>], days: &[i32], vals: &[[i64; 4]]) {
    let schema = Arc::new(
        parse_message_type(
            "message campaign_daily { required binary campaign_id (STRING); required binary channel (STRING);
             required int32 day (DATE); required int64 sessions; required int64 purchasing_sessions;
             required int64 revenue_cents; required int64 assisted_sessions; }",
        )
        .unwrap(),
    );
    let file = std::fs::File::create(path).expect("create output");
    let mut w = SerializedFileWriter::new(std::io::BufWriter::new(file), schema, props()).unwrap();
    if !ids.is_empty() {
        let mut rg = w.next_row_group().unwrap();
        let mut col = 0;
        while let Some(mut c) = rg.next_column().unwrap() {
            match col {
                0 | 1 => {
                    let src = if col == 0 { ids } else { chans };
                    let v: Vec<ByteArray> = src.iter().map(|s| ByteArray::from(s.clone())).collect();
                    c.typed::<ByteArrayType>().write_batch(&v, None, None).unwrap();
                }
                2 => {
                    c.typed::<Int32Type>().write_batch(days, None, None).unwrap();
                }
                k => {
                    let v: Vec<i64> = vals.iter().map(|r| r[k - 3]).collect();
                    c.typed::<Int64Type>().write_batch(&v, None, None).unwrap();
                }
            }
            c.close().unwrap();
            col += 1;
        }
        rg.close().unwrap();
    }
    w.close().unwrap();
}

// ---------------- per-person stage ----------------

struct Ctx {
    ndays: usize,
    ws_day: i64,
    organic: u16,
}

struct Acc {
    rows: Vec<UserRow>,
    credit: Vec<[i64; 3]>, // (camp incl organic) * ndays: sessions, purchasing, revenue
    assist: Vec<i64>,      // camp * ndays
    cnt: Vec<u32>,
    pos: Vec<u32>,
    active: Vec<u16>,
    catsum: Vec<i64>,
    touched: Vec<u16>,
    // grouping scratch, reused across buckets
    slot_id: Vec<u32>,
    p_ph: Vec<u64>,
    p_name: Vec<Name>,
    p_slot: Vec<u32>,
    p_ecnt: Vec<u32>,
    p_ccnt: Vec<u32>,
    ev_id: Vec<u32>,
    cl_id: Vec<u32>,
    ev_tmp: Vec<Ev>,
    cl_tmp: Vec<Click>,
}

impl Acc {
    fn new(ncamp: usize, ndays: usize, ncat: usize) -> Acc {
        Acc {
            rows: Vec::new(),
            credit: vec![[0i64; 3]; (ncamp + 1) * ndays],
            assist: vec![0i64; ncamp.max(1) * ndays],
            cnt: vec![0u32; ncamp.max(1)],
            pos: vec![u32::MAX; ncamp.max(1)],
            active: Vec::new(),
            catsum: vec![0i64; ncat],
            touched: Vec::new(),
            slot_id: Vec::new(),
            p_ph: Vec::new(),
            p_name: Vec::new(),
            p_slot: Vec::new(),
            p_ecnt: Vec::new(),
            p_ccnt: Vec::new(),
            ev_id: Vec::new(),
            cl_id: Vec::new(),
            ev_tmp: Vec::new(),
            cl_tmp: Vec::new(),
        }
    }
}

/// Sessions, credit and assists for one person: `ev` sorted by ts, `cl` sorted by (ts, click_id).
fn process_person(acc: &mut Acc, ev: &[Ev], cl: &[Click], gname: Name, cx: &Ctx) {
    let Acc { rows, credit, assist, cnt, pos, active, catsum, touched, .. } = acc;
    let ndays = cx.ndays;
    let (mut lo, mut hi) = (0usize, 0usize);
    let mut cur_day = i64::MIN;
    let (mut d_sess, mut d_ev, mut d_rev) = (0i64, 0i64, 0i64);
    let flush = |rows: &mut Vec<UserRow>, day: i64, s: i64, e: i64, r: i64, catsum: &mut [i64], touched: &mut Vec<u16>| {
        let mut top = u16::MAX;
        let mut best = i64::MIN;
        for &c in touched.iter() {
            let v = catsum[c as usize];
            if v > best || (v == best && c < top) {
                best = v;
                top = c;
            }
        }
        for &c in touched.iter() {
            catsum[c as usize] = 0;
        }
        touched.clear();
        rows.push(UserRow { name: gname, day: day as i32, sessions: s, events: e, rev: r, top });
    };
    let mut j = 0;
    while j < ev.len() {
        let start = ev[j].ts;
        let mut last = start;
        let (mut n, mut rev, mut np) = (0i64, 0i64, 0i64);
        let day = start.div_euclid(DAY);
        if day != cur_day {
            if cur_day != i64::MIN {
                flush(rows, cur_day, d_sess, d_ev, d_rev, catsum, touched);
            }
            cur_day = day;
            d_sess = 0;
            d_ev = 0;
            d_rev = 0;
        }
        while j < ev.len() && ev[j].ts - last <= GAP {
            let e = &ev[j];
            last = e.ts;
            n += 1;
            if e.purchase {
                np += 1;
                rev += e.price as i64;
                if catsum[e.cat as usize] == 0 && !touched.contains(&e.cat) {
                    touched.push(e.cat);
                }
                catsum[e.cat as usize] += e.price as i64;
            }
            j += 1;
        }
        d_sess += 1;
        d_ev += n;
        d_rev += rev;
        let di = (day - cx.ws_day) as usize;

        // clicks with ts <= start enter the window; those before start - 7 days leave it
        while hi < cl.len() && cl[hi].ts <= start {
            let c = cl[hi].camp as usize;
            if cnt[c] == 0 {
                pos[c] = active.len() as u32;
                active.push(c as u16);
            }
            cnt[c] += 1;
            hi += 1;
        }
        while lo < hi && cl[lo].ts < start - WEEK {
            let c = cl[lo].camp as usize;
            cnt[c] -= 1;
            if cnt[c] == 0 {
                let p = pos[c] as usize;
                let last_c = *active.last().unwrap();
                active.swap_remove(p);
                if p < active.len() {
                    pos[last_c as usize] = p as u32;
                }
            }
            lo += 1;
        }
        let camp = if hi > lo {
            // latest click at or before the start; on a tie, the smallest click_id (first in order)
            let t = cl[hi - 1].ts;
            let mut f = hi - 1;
            while f > lo && cl[f - 1].ts == t {
                f -= 1;
            }
            cl[f].camp
        } else {
            cx.organic
        };
        let cr = &mut credit[camp as usize * ndays + di];
        cr[0] += 1;
        cr[1] += (np > 0) as i64;
        cr[2] += rev;
        for &a in active.iter() {
            assist[a as usize * ndays + di] += 1;
        }
    }
    if cur_day != i64::MIN {
        flush(rows, cur_day, d_sess, d_ev, d_rev, catsum, touched);
    }
    for &a in active.iter() {
        cnt[a as usize] = 0;
    }
    active.clear();
}

#[inline]
fn sort_clicks(c: &mut [Click]) {
    c.sort_unstable_by(|a, b| a.ts.cmp(&b.ts).then_with(|| a.cid.cmp(b.cid)));
}

/// Fast path: group a bucket's events and clicks by person with a small hash table, then sort each
/// person's own events by time. Returns false (doing nothing) if two different names share a person hash.
fn group_bucket(acc: &mut Acc, evs: &[Ev], cls: &[Click], cx: &Ctx) -> bool {
    let n = evs.len();
    if n == 0 {
        return true;
    }
    let cap = (n * 2).next_power_of_two().max(16);
    let mask = cap - 1;
    if acc.slot_id.len() < cap {
        acc.slot_id.resize(cap, u32::MAX);
    }
    acc.p_ph.clear();
    acc.p_name.clear();
    acc.p_slot.clear();
    acc.p_ecnt.clear();
    acc.ev_id.clear();
    let mut ok = true;
    for e in evs {
        let mut s = (e.ph as usize) & mask;
        let id = loop {
            let k = acc.slot_id[s];
            if k == u32::MAX {
                let id = acc.p_ph.len() as u32;
                acc.slot_id[s] = id;
                acc.p_ph.push(e.ph);
                acc.p_name.push(e.name);
                acc.p_slot.push(s as u32);
                acc.p_ecnt.push(0);
                break id;
            }
            if acc.p_ph[k as usize] == e.ph {
                break k;
            }
            s = (s + 1) & mask;
        };
        if !acc.p_name[id as usize].same(&e.name) {
            ok = false;
        }
        acc.p_ecnt[id as usize] += 1;
        acc.ev_id.push(id);
    }
    let np = acc.p_ph.len();
    acc.p_ccnt.clear();
    acc.p_ccnt.resize(np, 0);
    acc.cl_id.clear();
    for c in cls {
        let mut s = (c.ph as usize) & mask;
        let id = loop {
            let k = acc.slot_id[s];
            if k == u32::MAX {
                break u32::MAX; // person has no events: the click cannot credit or assist anything
            }
            if acc.p_ph[k as usize] == c.ph {
                break k;
            }
            s = (s + 1) & mask;
        };
        if id != u32::MAX {
            if !acc.p_name[id as usize].same(&c.name) {
                ok = false;
            }
            acc.p_ccnt[id as usize] += 1;
        }
        acc.cl_id.push(id);
    }
    let clear_slots = |acc: &mut Acc| {
        for &s in &acc.p_slot {
            acc.slot_id[s as usize] = u32::MAX;
        }
    };
    if !ok {
        clear_slots(acc);
        return false;
    }
    // counting sort into person order
    let mut eoff = Vec::with_capacity(np + 1);
    let mut coff = Vec::with_capacity(np + 1);
    let (mut se, mut sc) = (0u32, 0u32);
    for p in 0..np {
        eoff.push(se);
        coff.push(sc);
        se += acc.p_ecnt[p];
        sc += acc.p_ccnt[p];
    }
    eoff.push(se);
    coff.push(sc);
    acc.ev_tmp.clear();
    acc.ev_tmp.reserve(n);
    acc.cl_tmp.clear();
    acc.cl_tmp.reserve(sc as usize);
    {
        let mut w = eoff.clone();
        let dst = acc.ev_tmp.as_mut_ptr();
        for (e, &id) in evs.iter().zip(&acc.ev_id) {
            unsafe { dst.add(w[id as usize] as usize).write(*e) };
            w[id as usize] += 1;
        }
        unsafe { acc.ev_tmp.set_len(n) };
        let mut w = coff.clone();
        let dst = acc.cl_tmp.as_mut_ptr();
        for (c, &id) in cls.iter().zip(&acc.cl_id) {
            if id != u32::MAX {
                unsafe { dst.add(w[id as usize] as usize).write(*c) };
                w[id as usize] += 1;
            }
        }
        unsafe { acc.cl_tmp.set_len(sc as usize) };
    }
    clear_slots(acc);
    let mut ev_tmp = std::mem::take(&mut acc.ev_tmp);
    let mut cl_tmp = std::mem::take(&mut acc.cl_tmp);
    for p in 0..np {
        let ev = &mut ev_tmp[eoff[p] as usize..eoff[p + 1] as usize];
        ev.sort_unstable_by_key(|e| e.ts);
        let cl = &mut cl_tmp[coff[p] as usize..coff[p + 1] as usize];
        sort_clicks(cl);
        let name = acc.p_name[p];
        process_person(acc, ev, cl, name, cx);
    }
    acc.ev_tmp = ev_tmp;
    acc.cl_tmp = cl_tmp;
    true
}

/// Exact path (used only when a person hash collides): sort by (hash, name, ts) and group by (hash, name).
fn process_bucket_sorted(acc: &mut Acc, evs: &mut [Ev], cls: &mut [Click], cx: &Ctx) {
    evs.sort_unstable_by(|a, b| a.ph.cmp(&b.ph).then(a.name.bytes().cmp(b.name.bytes())).then(a.ts.cmp(&b.ts)));
    cls.sort_unstable_by(|a, b| a.ph.cmp(&b.ph).then(a.name.bytes().cmp(b.name.bytes())).then(a.ts.cmp(&b.ts)).then(a.cid.cmp(b.cid)));
    let mut ci = 0usize;
    let mut i = 0usize;
    while i < evs.len() {
        let (kph, knm) = (evs[i].ph, evs[i].name);
        let mut gend = i + 1;
        while gend < evs.len() && evs[gend].ph == kph && evs[gend].name.same(&knm) {
            gend += 1;
        }
        while ci < cls.len() && (cls[ci].ph, cls[ci].name.bytes()) < (kph, knm.bytes()) {
            ci += 1;
        }
        let cs = ci;
        while ci < cls.len() && cls[ci].ph == kph && cls[ci].name.same(&knm) {
            ci += 1;
        }
        process_person(acc, &evs[i..gend], &cls[cs..ci], knm, cx);
        i = gend;
    }
}

// ---------------- main ----------------

fn main() {
    // test hook: `shop --parse-ts` reads strings (one per line) and prints parse_ts for each
    if std::env::args().nth(1).as_deref() == Some("--parse-ts") {
        use std::io::{BufRead, Write};
        let stdin = std::io::stdin();
        let mut out = std::io::BufWriter::new(std::io::stdout());
        for line in stdin.lock().split(b'\n') {
            let l = line.unwrap();
            let v = parse_ts(&l);
            if v == BAD {
                writeln!(out, "BAD").unwrap();
            } else {
                writeln!(out, "{}", v).unwrap();
            }
        }
        return;
    }
    let t0 = Instant::now();
    let data = PathBuf::from(std::env::var("DATA_DIR").unwrap_or_else(|_| "/data".into()));
    let out = PathBuf::from(std::env::var("OUTPUT_DIR").unwrap_or_else(|_| "/output".into()));
    let ncpu: usize = std::env::var("NCPU").ok().and_then(|s| s.parse().ok()).unwrap_or(0);
    rayon::ThreadPoolBuilder::new().num_threads(ncpu).build_global().unwrap();

    let (ws, we) = read_manifest(&data);
    let cws = ws - WEEK;
    let ws_day = ws.div_euclid(DAY);
    let ndays = ((we - 1).div_euclid(DAY) - ws_day + 1).max(1) as usize;

    // reference tables
    let prods = read_ref::<2>(&data.join("products.csv"));
    let camps = read_ref::<2>(&data.join("campaigns.csv"));
    let mut catnames: Vec<Vec<u8>> = prods.iter().map(|r| r[1].to_vec()).collect();
    catnames.push(b"unknown".to_vec());
    catnames.sort();
    catnames.dedup();
    let cat_of: FxHashMap<&[u8], u16> = catnames.iter().enumerate().map(|(i, c)| (&c[..], i as u16)).collect();
    let unknown = cat_of[&b"unknown"[..]];
    let prod_cat: StrMap<u16> = StrMap::new(prods.iter().map(|r| (r[0], cat_of[r[1]])), u16::MAX);
    let camp_ids: Vec<Vec<u8>> = camps.iter().map(|r| r[0].to_vec()).collect();
    let camp_chan: Vec<Vec<u8>> = camps.iter().map(|r| r[1].to_vec()).collect();
    let camp_of: StrMap<u16> = StrMap::new(camps.iter().enumerate().map(|(i, r)| (r[0], i as u16)), u16::MAX);
    let ncamp = camps.len();
    let organic = ncamp as u16;

    // events: memory-mapped, parsed in parallel slices of each file
    let ev_map = map_files(&data.join("events"));

    // links, and at the same time pass 1 over events: hash every event_id (first field only) to learn
    // which ids occur more than once. The links work has serial steps; pass 1 fills the idle threads.
    let (people, id_parts) = rayon::join(|| build_people(&data), || -> Vec<Vec<Vec<u64>>> { ev_map
        .tasks
        .par_iter()
        .map(|t| {
            let c = task_lines(&ev_map, t);
            let per = c.len() / 70 / DB * 5 / 4;
            let mut v: Vec<Vec<u64>> = (0..DB).map(|_| Vec::with_capacity(per)).collect();
            for_lines(c, |l| {
                let f = match memchr(b',', l) {
                    Some(i) => &l[..i],
                    None => l,
                };
                // same unquoting as the full parser for any line it accepts
                let f = if f.len() >= 2 && f[0] == b'"' && f[f.len() - 1] == b'"' { &f[1..f.len() - 1] } else { f };
                if f.is_empty() {
                    return;
                }
                let eh = hash_bytes(f);
                v[(eh >> (64 - DB_BITS)) as usize].push(eh);
            });
            v
        })
        .collect() });
    log!(t0, "links: {} ids; events pass 1 hashed", people.n);
    let id_parts = &id_parts;
    // events (pass 2, dedup, bucketing) and clicks run side by side: each has serial steps the other fills
    let ((ev_parts, eoffs), (cl_parts, coffs)) = rayon::join(|| {
    let dups: Vec<Vec<u64>> = (0..DB)
        .into_par_iter()
        .map(|b| {
            let n: usize = id_parts.iter().map(|t| t[b].len()).sum();
            let mut seen: rustc_hash::FxHashSet<u64> = rustc_hash::FxHashSet::with_capacity_and_hasher(n, Default::default());
            let mut d = Vec::new();
            for &h in id_parts.iter().flat_map(|t| t[b].iter()) {
                if !seen.insert(h) {
                    d.push(h);
                }
            }
            d
        })
        .collect();
    let ndup: usize = dups.iter().map(|d| d.len()).sum();
    let dbits_log = ((ndup.max(1) * 16).next_power_of_two().trailing_zeros()).clamp(16, 34);
    let mut dbits = vec![0u64; (1usize << dbits_log) / 64];
    for d in &dups {
        for &h in d {
            let i = (h >> (64 - dbits_log)) as usize;
            dbits[i >> 6] |= 1u64 << (i & 63);
        }
    }
    let dbits = &dbits;
    let maybe_dup = |eh: u64| -> bool {
        let i = (eh >> (64 - dbits_log)) as usize;
        dbits[i >> 6] >> (i & 63) & 1 != 0
    };
    let prefetch_dup = |eh: u64| {
        #[cfg(target_arch = "x86_64")]
        unsafe {
            let i = (eh >> (64 - dbits_log)) as usize;
            std::arch::x86_64::_mm_prefetch(dbits.as_ptr().add(i >> 6) as *const i8, std::arch::x86_64::_MM_HINT_T0)
        };
    };
    log!(t0, "events pass 1: {} repeated ids", ndup);

    // validate a kept copy and resolve its person
    let to_ev = |e: &RawEv| -> Option<Ev> {
        if e.uid.is_empty() || e.kind > 2 || e.ts == BAD || e.ts < ws || e.ts >= we {
            return None;
        }
        if e.kind == 2 && e.price < 0 {
            return None;
        }
        if e.its - e.ts > DAY {
            return None;
        }
        // person resolved later in batches: carry the user id's hash and bytes for now
        Some(Ev { ph: e.uh, name: Name::new(e.uid), ts: e.ts, price: e.price, cat: e.cat, purchase: e.kind == 2 })
    };

    // pass 2: full parse. Events whose id occurs once are final right away; the rest go to exact dedup.
    let pass2: Vec<(Vec<Ev>, Vec<Vec<RawEv>>)> = ev_map
        .tasks
        .par_iter()
        .map(|t| {
            let c = task_lines(&ev_map, t);
            let mut out: Vec<Ev> = Vec::with_capacity(c.len() / 75 + 16);
            let mut dv: Vec<Vec<RawEv>> = (0..DB).map(|_| Vec::new()).collect();
            // events wait in a short batch so the duplicate-bit lookups (a few MB, random) overlap
            const BATCH: usize = 32;
            let mut batch: Vec<RawEv> = Vec::with_capacity(BATCH);
            let place = |e: &RawEv, out: &mut Vec<Ev>, dv: &mut Vec<Vec<RawEv>>| {
                if maybe_dup(e.eh) {
                    dv[(e.eh >> (64 - DB_BITS)) as usize].push(*e);
                } else if let Some(v) = to_ev(e) {
                    out.push(v);
                }
            };
            parse_lines::<7>(c, |&[eid, uid, ts, its, et, pid, pr]| {
                if eid.is_empty() {
                    return;
                }
                let mut kind = match et {
                    b"view" => 0,
                    b"cart" => 1,
                    b"purchase" => 2,
                    _ => 3,
                };
                if pid.is_empty() {
                    kind = 4;
                }
                let eh = hash_bytes(eid);
                let e = RawEv {
                    eh,
                    eid,
                    uid,
                    uh: hash_bytes(uid),
                    its: parse_ts(its),
                    ts: parse_ts(ts),
                    price: if kind == 2 { parse_price(pr) } else { 0 },
                    // category only matters for purchases (top_category); other events just need a product id
                    cat: if kind == 2 { prod_cat.get(pid).unwrap_or(unknown) } else { 0 },
                    kind,
                };
                prefetch_dup(eh);
                batch.push(e);
                if batch.len() == BATCH {
                    for e in &batch {
                        place(e, &mut out, &mut dv);
                    }
                    batch.clear();
                }
            });
            for e in &batch {
                place(e, &mut out, &mut dv);
            }
            people.resolve_all(&mut out, |e: &Ev| (e.ph, e.name.bytes()), |e: &mut Ev, ph, name| {
                e.ph = ph;
                e.name = name;
            });
            (out, dv)
        })
        .collect();
    log!(t0, "events pass 2 parsed: {} slices", ev_map.tasks.len());

    // exact dedup of repeated ids (earliest ingest_ts wins), then validate the kept copy
    let pass2 = pass2;
    let dedup_out: Vec<Vec<Ev>> = (0..DB)
        .into_par_iter()
        .with_min_len(4)
        .fold(
            || (Vec::<Ev>::new(), FxHashMap::<u64, u32>::default(), Vec::<RawEv>::new()),
            |(mut out, mut seen, mut keep), b| {
                seen.clear();
                keep.clear();
                for e in pass2.iter().flat_map(|t| t.1[b].iter()) {
                    match seen.get(&e.eh) {
                        None => {
                            seen.insert(e.eh, keep.len() as u32);
                            keep.push(*e);
                        }
                        Some(&i) => {
                            let i = i as usize;
                            if eq_bytes(keep[i].eid, e.eid) {
                                if e.its < keep[i].its {
                                    keep[i] = *e;
                                }
                            } else {
                                // two different ids share a 64-bit hash: compare exactly
                                match keep.iter().position(|k| k.eid == e.eid) {
                                    Some(j) => {
                                        if e.its < keep[j].its {
                                            keep[j] = *e;
                                        }
                                    }
                                    None => keep.push(*e),
                                }
                            }
                        }
                    }
                }
                let before = out.len();
                for e in &keep {
                    if let Some(v) = to_ev(e) {
                        out.push(v);
                    }
                }
                people.resolve_all(&mut out[before..], |e: &Ev| (e.ph, e.name.bytes()), |e: &mut Ev, ph, name| {
                    e.ph = ph;
                    e.name = name;
                });
                (out, seen, keep)
            },
        )
        .map(|(out, _, _)| out)
        .collect();
    let mut ev_parts: Vec<Vec<Ev>> = Vec::with_capacity(pass2.len() + dedup_out.len());
    for (o, _) in pass2 {
        ev_parts.push(o);
    }
    ev_parts.extend(dedup_out);
    log!(t0, "events deduped and cleaned");
    let eoffs = bucket_parts(&mut ev_parts, PB, |e: &Ev| pbucket(e.ph));
    log!(t0, "events bucketed by person: {}", ev_parts.iter().map(|p| p.len()).sum::<usize>());
    (ev_parts, eoffs)
    }, || {

    // clicks (copies are identical, so duplicates change nothing and need no dedup)
    let cl_map = map_files(&data.join("clicks"));
    let mut cl_parts: Vec<Vec<Click>> = cl_map
        .tasks
        .par_iter()
        .map(|t| {
            let c = task_lines(&cl_map, t);
            let mut v: Vec<Click> = Vec::with_capacity(c.len() / 45 + 16);
            parse_lines::<4>(c, |&[cid, uid, camp, ts]| {
                if cid.is_empty() || uid.is_empty() {
                    return;
                }
                let Some(ci) = camp_of.get(camp) else { return };
                let tm = parse_ts(ts);
                if tm == BAD || tm < cws || tm >= we {
                    return;
                }
                v.push(Click { ph: hash_bytes(uid), name: Name::new(uid), ts: tm, cid, camp: ci });
            });
            people.resolve_all(&mut v, |c: &Click| (c.ph, c.name.bytes()), |c: &mut Click, ph, name| {
                c.ph = ph;
                c.name = name;
            });
            v
        })
        .collect();
    let coffs = bucket_parts(&mut cl_parts, PB, |c: &Click| pbucket(c.ph));
    log!(t0, "clicks cleaned and bucketed: {}", cl_parts.iter().map(|p| p.len()).sum::<usize>());
    (cl_parts, coffs)
    });

    let ncat = catnames.len();
    let force_exact = std::env::var_os("SHOP_EXACT").is_some();
    let ctx = Ctx { ndays, ws_day, organic };
    let (ev_parts, cl_parts) = (&ev_parts, &cl_parts);
    let (eoffs, coffs) = (&eoffs, &coffs);
    let results: Vec<Acc> = (0..PB)
        .into_par_iter()
        .fold(
            || (Acc::new(ncamp, ndays, ncat), Vec::<Ev>::new(), Vec::<Click>::new()),
            |(mut acc, mut ev, mut cl), b| {
                // copy the bucket's pieces into one reused, cache-sized buffer
                ev.clear();
                for p in ev_parts.iter().zip(eoffs) {
                    ev.extend_from_slice(&p.0[p.1[b] as usize..p.1[b + 1] as usize]);
                }
                cl.clear();
                for p in cl_parts.iter().zip(coffs) {
                    cl.extend_from_slice(&p.0[p.1[b] as usize..p.1[b + 1] as usize]);
                }
                if force_exact || !group_bucket(&mut acc, &ev, &cl, &ctx) {
                    process_bucket_sorted(&mut acc, &mut ev, &mut cl, &ctx);
                }
                (acc, ev, cl)
            },
        )
        .map(|(acc, _, _)| acc)
        .collect();
    log!(t0, "sessions, credit and assists done");

    // campaign_daily
    let mut credit = vec![[0i64; 3]; (ncamp + 1) * ndays];
    let mut assist = vec![0i64; ncamp.max(1) * ndays];
    for r in &results {
        for (a, b) in credit.iter_mut().zip(&r.credit) {
            a[0] += b[0];
            a[1] += b[1];
            a[2] += b[2];
        }
        for (a, b) in assist.iter_mut().zip(&r.assist) {
            *a += b;
        }
    }
    let (mut ids, mut chans, mut days, mut vals) = (Vec::new(), Vec::new(), Vec::new(), Vec::new());
    for c in 0..=ncamp {
        for d in 0..ndays {
            let cr = credit[c * ndays + d];
            let a = if c < ncamp { assist[c * ndays + d] } else { 0 };
            if cr[0] > 0 || a > 0 {
                if c < ncamp {
                    ids.push(camp_ids[c].clone());
                    chans.push(camp_chan[c].clone());
                } else {
                    ids.push(b"organic".to_vec());
                    chans.push(b"organic".to_vec());
                }
                days.push((ws_day + d as i64) as i32);
                vals.push([cr[0], cr[1], cr[2], a]);
            }
        }
    }
    let ud = out.join("user_daily");
    let cd = out.join("campaign_daily");
    std::fs::create_dir_all(&ud).expect("mkdir user_daily");
    std::fs::create_dir_all(&cd).expect("mkdir campaign_daily");
    write_campaign(&cd.join("part-0.parquet"), &ids, &chans, &days, &vals);

    // user_daily: one file per group of buckets, written in parallel
    let all: Vec<&UserRow> = results.iter().flat_map(|r| r.rows.iter()).collect();
    let nfiles = rayon::current_num_threads().max(1);
    let per = all.len().div_ceil(nfiles).max(1);
    let groups: Vec<&[&UserRow]> = if all.is_empty() { vec![&all[..]] } else { all.chunks(per).collect() };
    groups.par_iter().enumerate().for_each(|(i, g)| {
        write_user(&ud.join(format!("part-{i}.parquet")), g, &catnames);
    });
    let hwm = std::fs::read_to_string("/proc/self/status").ok().and_then(|st| st.lines().find(|l| l.starts_with("VmHWM")).map(|l| l.to_string())).unwrap_or_default();
    log!(t0, "written: {} user rows, {} campaign rows; peak memory {}", all.len(), ids.len(), hwm.trim());
    std::process::exit(0);
}
