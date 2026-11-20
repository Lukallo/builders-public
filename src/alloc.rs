// Global allocator: big blocks are 2 MiB aligned and advised to use transparent
// huge pages, which cuts page-fault and TLB costs for the large record arrays.
use std::alloc::{GlobalAlloc, Layout, System};

pub struct Huge;
const BIG: usize = 4 << 20;
const ALIGN: usize = 2 << 20;
unsafe extern "C" {
    fn madvise(addr: *mut u8, len: usize, advice: i32) -> i32;
}

unsafe impl GlobalAlloc for Huge {
    unsafe fn alloc(&self, l: Layout) -> *mut u8 {
        unsafe {
            if l.size() >= BIG {
                let l2 = Layout::from_size_align_unchecked(l.size(), ALIGN.max(l.align()));
                let p = System.alloc(l2);
                if !p.is_null() {
                    madvise(p, l.size() & !(ALIGN - 1), 14);
                }
                p
            } else {
                System.alloc(l)
            }
        }
    }
    unsafe fn dealloc(&self, p: *mut u8, l: Layout) {
        unsafe {
            if l.size() >= BIG {
                System.dealloc(p, Layout::from_size_align_unchecked(l.size(), ALIGN.max(l.align())))
            } else {
                System.dealloc(p, l)
            }
        }
    }
    unsafe fn alloc_zeroed(&self, l: Layout) -> *mut u8 {
        unsafe {
            if l.size() >= BIG {
                let l2 = Layout::from_size_align_unchecked(l.size(), ALIGN.max(l.align()));
                let p = System.alloc_zeroed(l2);
                if !p.is_null() {
                    madvise(p, l.size() & !(ALIGN - 1), 14);
                }
                p
            } else {
                System.alloc_zeroed(l)
            }
        }
    }
    unsafe fn realloc(&self, p: *mut u8, l: Layout, new: usize) -> *mut u8 {
        unsafe {
            if l.size() < BIG && new < BIG {
                return System.realloc(p, l, new);
            }
            let nl = Layout::from_size_align_unchecked(new, l.align());
            let q = self.alloc(nl);
            if !q.is_null() {
                std::ptr::copy_nonoverlapping(p, q, l.size().min(new));
                self.dealloc(p, l);
            }
            q
        }
    }
}
