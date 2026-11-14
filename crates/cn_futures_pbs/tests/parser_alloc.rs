use std::{
    alloc::{GlobalAlloc, Layout, System},
    cell::Cell,
};
thread_local! {static ENABLED:Cell<bool>=const{Cell::new(false)};static COUNT:Cell<usize>=const{Cell::new(0)};}
struct Allocator;
unsafe impl GlobalAlloc for Allocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        if ENABLED.try_with(Cell::get).unwrap_or(false) {
            let _ = COUNT.try_with(|n| n.set(n.get() + 1));
        }
        System.alloc(layout)
    }
    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        System.dealloc(ptr, layout)
    }
    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new: usize) -> *mut u8 {
        if ENABLED.try_with(Cell::get).unwrap_or(false) {
            let _ = COUNT.try_with(|n| n.set(n.get() + 1));
        }
        System.realloc(ptr, layout, new)
    }
}
#[global_allocator]
static ALLOCATOR: Allocator = Allocator;
#[test]
fn raw_parser_allocates_nothing() {
    let frames: [&[u8]; 6] = [
        include_bytes!("fixtures/mdl_21_1.txt"),
        include_bytes!("fixtures/mdl_22_1.txt"),
        include_bytes!("fixtures/mdl_22_3.txt"),
        include_bytes!("fixtures/mdl_23_1.txt"),
        include_bytes!("fixtures/mdl_24_1.txt"),
        include_bytes!("fixtures/mdl_26_1.txt"),
    ];
    COUNT.with(|n| n.set(0));
    ENABLED.with(|b| b.set(true));
    for _ in 0..1000 {
        for frame in frames {
            std::hint::black_box(
                cn_futures_pbs::sources::zmq::parse(std::hint::black_box(frame), 1).unwrap(),
            );
        }
    }
    ENABLED.with(|b| b.set(false));
    assert_eq!(COUNT.with(Cell::get), 0);
}
