//! The `skyfs` command: create, mount (FUSE on Linux, NFS on macOS), fsck,
//! info.

mod cli;
#[cfg(target_os = "linux")]
mod fuse;
mod nfs;
mod signal;

use std::process::ExitCode;

use clap::Parser;
use skyfs_store::{ObjectClient, VolumeRoot, backend, hex};
use skyfs_volume::{AtimeMode, CreateOptions, FsyncMode, Replication, VolumeConfig};

use cli::{
    AtimeArg, Cli, Command, FsyncArg, MountArgs, ReplicationArg, StoreArgs, Transport,
    parse_volume_id,
};

type Result<T> = std::result::Result<T, String>;

fn client(
    store: &StoreArgs,
    volume_id: [u8; 16],
    writer_id: &str,
    read_only: bool,
) -> Result<ObjectClient> {
    let cfg = store.config()?;
    let b = backend::open(&cfg, &volume_id, writer_id, read_only).map_err(|e| e.to_string())?;
    Ok(ObjectClient::new(
        b.store,
        VolumeRoot::new(&b.prefix, &volume_id),
    ))
}

fn rt() -> Result<tokio::runtime::Runtime> {
    tokio::runtime::Runtime::new().map_err(|e| e.to_string())
}

fn run(cli: Cli) -> Result<()> {
    match cli.command {
        Command::Create {
            store,
            volume,
            inline_data_max,
            root_uid,
            root_gid,
        } => {
            let id = match volume {
                Some(v) => parse_volume_id(&v)?,
                None => rand::random(),
            };
            let c = client(&store, id, "skyfs-create", false)?;
            let scratch = std::env::temp_dir().join(format!("skyfs-create-{}", hex(&id)));
            let opts = CreateOptions {
                inline_data_max,
                root_uid,
                root_gid,
                ..CreateOptions::default()
            };
            skyfs_volume::create_volume(&c, id, &opts, &scratch).map_err(|e| e.to_string())?;
            let _ = std::fs::remove_dir_all(scratch);
            println!("{}", hex(&id));
            Ok(())
        }
        Command::Fsck { store, volume } => {
            let id = parse_volume_id(&volume)?;
            let c = client(&store, id, "skyfs-fsck", true)?;
            let scratch = std::env::temp_dir().join(format!("skyfs-fsck-{}", hex(&id)));
            let (problems, head) = rt()?
                .block_on(skyfs_volume::mount::fsck(&c, id, &scratch))
                .map_err(|e| e.to_string())?;
            let _ = std::fs::remove_dir_all(scratch);
            if problems.is_empty() {
                println!("ok: replayed through segment {head}, no problems");
                Ok(())
            } else {
                for p in &problems {
                    println!("{p}");
                }
                Err(format!("{} problems", problems.len()))
            }
        }
        Command::Info { store, volume } => {
            let id = parse_volume_id(&volume)?;
            let c = client(&store, id, "skyfs-info", true)?;
            rt()?.block_on(async {
                let hint = skyfs_volume::snapshot::read_hint(&c).await;
                let m = skyfs_volume::snapshot::latest(&c, hint, &id)
                    .await
                    .map_err(|e| e.to_string())?
                    .ok_or("no volume here")?;
                println!("volume           {}", hex(&id));
                println!("root             {}", c.root());
                println!("snapshot gen     {}", m.r#gen);
                println!("restored seq     {}", m.restored_seq);
                println!("restored op      {}", m.restored_op_id);
                println!("inodes           {}", m.inode_count);
                println!("data bytes       {}", m.data_bytes);
                println!("hint head seq    {}", hint.head_seq);
                Ok(())
            })
        }
        Command::Mount(args) => mount(&args),
    }
}

fn volume_config(args: &MountArgs) -> VolumeConfig {
    let mut cfg = VolumeConfig::new(args.cache_dir.clone());
    cfg.replication = match args.replication {
        ReplicationArg::Sync => Replication::Sync,
        ReplicationArg::Async => Replication::Async,
    };
    // The NFS client commits at every close, not only at fsync: waiting for
    // the bucket there would make each close a network round trip.
    let fsync = args.fsync.unwrap_or(match args.transport {
        Transport::Fuse => FsyncArg::Remote,
        Transport::Nfs => FsyncArg::Local,
    });
    cfg.fsync = match fsync {
        FsyncArg::Remote => FsyncMode::Remote,
        FsyncArg::Local => FsyncMode::Local,
    };
    cfg.max_lag = args.max_lag;
    cfg.restore_on_exit_timeout = args.restore_on_exit_timeout;
    cfg.retention = args.retention;
    cfg.cache_limit = args.cache_gib << 30;
    cfg.read_only = args.read_only;
    cfg.atime = match args.atime {
        AtimeArg::Relatime => AtimeMode::Relatime,
        AtimeArg::Noatime => AtimeMode::NoAtime,
        AtimeArg::Strictatime => AtimeMode::Strict,
    };
    cfg
}

fn mount(args: &MountArgs) -> Result<()> {
    let id = parse_volume_id(&args.volume)?;
    let cfg = volume_config(args);
    match args.transport {
        Transport::Nfs => {
            let c = client(&args.store, id, &cfg.writer_id, args.read_only)?;
            nfs::serve(c, id, cfg, args)
        }
        #[cfg(target_os = "linux")]
        Transport::Fuse => {
            let c = client(&args.store, id, &cfg.writer_id, args.read_only)?;
            fuse::serve(c, id, cfg, args)
        }
        #[cfg(not(target_os = "linux"))]
        Transport::Fuse => Err(
            "FUSE mounts are Linux-only here; use --transport nfs (the default on macOS)".into(),
        ),
    }
}

fn main() -> ExitCode {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "skyfs=info,skyfs_volume=info".into()),
        )
        .with_writer(std::io::stderr)
        .with_ansi(std::io::IsTerminal::is_terminal(&std::io::stderr()))
        .init();
    match run(Cli::parse()) {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("skyfs: {e}");
            ExitCode::FAILURE
        }
    }
}
