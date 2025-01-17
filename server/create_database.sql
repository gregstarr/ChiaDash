create table jobs (
    start_time text not null,
    harvester_ip text not null,
    temp_dir1 text not null,
    temp_dir2 text not null,
    final_dir text not null,
    plot_id text unique,
    process_id text not null,
    phase1_time real,
    phase2_time real,
    phase3_time real,
    phase4_time real,
    total_time real,
    copy_time real,
    plot_size int not null,
    buffer_size text not null,
    n_buckets int not null,
    n_threads int not null,
    stripe_size int not null,
    status text not null
);
