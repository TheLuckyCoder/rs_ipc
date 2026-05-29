use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use rs_ipc::SharedMessageMapper;
use std::ffi::CString;
use std::hint::black_box;
use std::sync::Arc;
use std::thread;

const DEFAULT_SIZE: usize = 1024 * 1024; // 1 MiB

fn get_test_data(size: usize) -> Vec<u8> {
    let mut data = vec![0u8; size];
    for i in 0..data.len() {
        data[i] = (i % 255) as u8;
    }
    data
}

fn pure_write(c: &mut Criterion) {
    let data = get_test_data(DEFAULT_SIZE);
    let name = CString::new("bench_pure_write").unwrap();
    let mapper =
        SharedMessageMapper::create(name, rs_ipc::SharedMessage::size_of_fields() + DEFAULT_SIZE)
            .unwrap();
    mapper.set_target_read_count(0);

    c.bench_function("pure_write", |b| {
        b.iter(|| {
            mapper.write_slice(black_box(&data));
        })
    });

    mapper.stop();
}

fn read_fast_path(c: &mut Criterion) {
    let data = get_test_data(DEFAULT_SIZE);
    let name = CString::new("bench_pure_read").unwrap();
    let mapper = Arc::new(
        SharedMessageMapper::create(name, rs_ipc::SharedMessage::size_of_fields() + DEFAULT_SIZE)
            .unwrap(),
    );
    mapper.set_target_read_count(0);
    mapper.add_reader();

    // Writer thread keeps producing fresh messages
    let mapper_writer = mapper.clone();
    thread::spawn(move || {
        while !mapper_writer.is_stopped() {
            mapper_writer.write_slice(&data);
        }
    });

    // Wait for first message to be available
    while mapper.read(0, false).is_none() {
        thread::yield_now();
    }

    let mut version = 0;
    c.bench_function("read_fast_path", |b| {
        b.iter(|| {
            if let Some(guard) = mapper.read(version, true) {
                version = guard.sequence();
                black_box(guard.data());
            }
        })
    });

    mapper.stop();
}

fn write_with_reader(c: &mut Criterion) {
    let data = get_test_data(DEFAULT_SIZE);
    let name = CString::new("bench_write_reader").unwrap();
    let mapper = Arc::new(
        SharedMessageMapper::create(name, rs_ipc::SharedMessage::size_of_fields() + DEFAULT_SIZE)
            .unwrap(),
    );
    mapper.set_target_read_count(0);
    mapper.add_reader();

    let mapper_clone = mapper.clone();
    thread::spawn(move || {
        let mut version = 0;
        while !mapper_clone.is_stopped() {
            if let Some(guard) = mapper_clone.read(version, true) {
                version = guard.sequence();
                black_box(guard.data());
            }
        }
    });

    c.bench_function("write_no_wait_with_reader", |b| {
        b.iter(|| {
            mapper.write_slice(black_box(&data));
        })
    });

    mapper.stop();
}

fn write_multiple_readers(c: &mut Criterion, readers: u16) {
    let data = get_test_data(DEFAULT_SIZE);
    let name = CString::new(format!("bench_write_readers_{}", readers)).unwrap();
    let mapper = Arc::new(
        SharedMessageMapper::create(name, rs_ipc::SharedMessage::size_of_fields() + DEFAULT_SIZE)
            .unwrap(),
    );
    mapper.set_target_read_count(readers);

    for _ in 0..readers {
        mapper.add_reader();
        let mapper_clone = mapper.clone();
        thread::spawn(move || {
            let mut version = 0;
            while !mapper_clone.is_stopped() {
                if let Some(guard) = mapper_clone.read(version, true) {
                    version = guard.sequence();
                    black_box(guard.data());
                }
            }
        });
    }

    c.bench_function(&format!("write_waiting_with_{:02}_readers", readers), |b| {
        b.iter(|| {
            mapper.write_slice(black_box(&data));
        })
    });

    mapper.stop();
}

fn bench_readers(c: &mut Criterion) {
    write_multiple_readers(c, 1);
    write_multiple_readers(c, 2);
    write_multiple_readers(c, 3);
    write_multiple_readers(c, 5);
    write_multiple_readers(c, 10);
}

fn bench_size_scaling(c: &mut Criterion) {
    let sizes: &[(usize, &str)] = &[
        (64 * 1024, "64KiB"),
        (256 * 1024, "256KiB"),
        (1024 * 1024, "1MiB"),
        (4 * 1024 * 1024, "4MiB"),
        (16 * 1024 * 1024, "16MiB"),
    ];

    let mut group = c.benchmark_group("size_scaling");
    for &(size, label) in sizes {
        let data = get_test_data(size);
        let name = CString::new(format!("bench_size_{}", label)).unwrap();
        let mapper = Arc::new(
            SharedMessageMapper::create(name, rs_ipc::SharedMessage::size_of_fields() + size)
                .unwrap(),
        );
        mapper.set_target_read_count(1);
        mapper.add_reader();

        let mapper_clone = mapper.clone();
        thread::spawn(move || {
            let mut version = 0;
            while !mapper_clone.is_stopped() {
                if let Some(guard) = mapper_clone.read(version, true) {
                    version = guard.sequence();
                    black_box(guard.data());
                }
            }
        });

        group.bench_with_input(
            BenchmarkId::new("write_roundtrip", label),
            &data,
            |b, data| {
                b.iter(|| {
                    mapper.write_slice(black_box(data));
                })
            },
        );

        mapper.stop();
    }
    group.finish();
}

criterion_group!(
    name = benches;
    config = Criterion::default().sample_size(50).measurement_time(std::time::Duration::from_secs(2));
    targets = pure_write, read_fast_path, write_with_reader, bench_readers, bench_size_scaling
);
criterion_main!(benches);
