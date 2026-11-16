//! Property: for random operation sequences, the live store equals a
//! replay of the encoded segments, both pass fsck, and every file's bytes
//! equal a reference model.

#![allow(
    clippy::many_single_char_names,
    clippy::unused_self,
    clippy::cast_possible_truncation,
    clippy::too_many_lines
)]

mod common;

use common::Harness;
use proptest::prelude::*;
use skyfs_format::wal::v1::{FallocateMode, RenameMode, XattrMode};
use skyfs_meta::{Ino, Kind, ROOT_INO};

const NAMES: [&str; 4] = ["a", "b", "c", "d"];

#[derive(Debug, Clone)]
enum Action {
    Mkdir(u8, u8),
    Create(u8, u8),
    Symlink(u8, u8),
    Write {
        file: u8,
        off: u16,
        len: u16,
        seed: u8,
    },
    Truncate(u8, u16),
    Unlink(u8, bool),
    Rmdir(u8),
    Rename {
        entry: u8,
        dir: u8,
        name: u8,
        mode: u8,
    },
    Link(u8, u8, u8),
    Fallocate {
        file: u8,
        off: u16,
        len: u16,
        mode: u8,
    },
    Clone {
        src: u8,
        dst: u8,
        so: u16,
        d_off: u16,
        len: u16,
    },
    Xattr(u8, u8, bool),
    Seal,
    CloseAll,
    Remount,
}

fn action() -> impl Strategy<Value = Action> {
    let i = any::<u8>;
    prop_oneof![
        3 => (i(), i()).prop_map(|(a, b)| Action::Mkdir(a, b)),
        4 => (i(), i()).prop_map(|(a, b)| Action::Create(a, b)),
        1 => (i(), i()).prop_map(|(a, b)| Action::Symlink(a, b)),
        6 => (i(), 0..3000u16, 1..2500u16, i())
            .prop_map(|(file, off, len, seed)| Action::Write { file, off, len, seed }),
        2 => (i(), 0..4000u16).prop_map(|(a, s)| Action::Truncate(a, s)),
        2 => (i(), any::<bool>()).prop_map(|(a, c)| Action::Unlink(a, c)),
        1 => i().prop_map(Action::Rmdir),
        3 => (i(), i(), i(), 0..3u8).prop_map(|(entry, dir, name, mode)| Action::Rename { entry, dir, name, mode }),
        1 => (i(), i(), i()).prop_map(|(a, b, c)| Action::Link(a, b, c)),
        2 => (i(), 0..3000u16, 1..2000u16, 0..7u8)
            .prop_map(|(file, off, len, mode)| Action::Fallocate { file, off, len, mode }),
        2 => (i(), i(), 0..3000u16, 0..3000u16, 0..2000u16)
            .prop_map(|(src, dst, so, d_off, len)| Action::Clone { src, dst, so, d_off, len }),
        1 => (i(), i(), any::<bool>()).prop_map(|(a, b, c)| Action::Xattr(a, b, c)),
        2 => Just(Action::Seal),
        1 => Just(Action::CloseAll),
        1 => Just(Action::Remount),
    ]
}

fn pick<T: Clone>(items: &[T], i: u8) -> Option<T> {
    (!items.is_empty()).then(|| items[usize::from(i) % items.len()].clone())
}

fn run(actions: &[Action]) {
    let mut h = Harness::new();
    for a in actions {
        // (parent, name, ino, kind) of every entry, and directories.
        let entries: Vec<(Ino, String, Ino, Kind)> = {
            let tree = h.tree(&h.live);
            tree.iter()
                .map(|(path, &(ino, kind))| {
                    let (dir, name) = path.rsplit_once('/').unwrap();
                    (h.resolve(dir).unwrap(), name.to_owned(), ino, kind)
                })
                .collect()
        };
        let mut dirs = vec![ROOT_INO];
        dirs.extend(
            entries
                .iter()
                .filter(|e| e.3 == Kind::Directory)
                .map(|e| e.2),
        );
        let files: Vec<Ino> = h
            .model
            .keys()
            .copied()
            .collect::<std::collections::BTreeSet<_>>()
            .into_iter()
            .collect();
        let name = |i: u8| NAMES[usize::from(i) % NAMES.len()];

        // Errors are expected and simply mean nothing was logged.
        let _ = match a.clone() {
            Action::Mkdir(d, n) => h.mkdir(pick(&dirs, d).unwrap(), name(n)).map(drop),
            Action::Create(d, n) => h.create(pick(&dirs, d).unwrap(), name(n)).map(drop),
            Action::Symlink(d, n) => h
                .make(pick(&dirs, d).unwrap(), name(n), Kind::Symlink, "target")
                .map(drop),
            Action::Write {
                file,
                off,
                len,
                seed,
            } => match pick(&files, file) {
                Some(f) => {
                    let bytes: Vec<u8> = (0..len).map(|i| seed.wrapping_add(i as u8)).collect();
                    h.write(f, u64::from(off), &bytes)
                }
                None => Ok(()),
            },
            Action::Truncate(f, s) => {
                pick(&files, f).map_or(Ok(()), |f| h.truncate(f, u64::from(s)))
            }
            Action::Unlink(e, close) => match pick(&entries, e) {
                Some((p, n, _, Kind::Directory)) => h.unlink_closed(p, &n, true),
                Some((p, n, _, _)) if close => h.unlink_closed(p, &n, false),
                Some((p, n, _, _)) => h.remove(p, &n, false).map(drop),
                None => Ok(()),
            },
            Action::Rmdir(e) => {
                pick(&entries, e).map_or(Ok(()), |(p, n, _, _)| h.unlink_closed(p, &n, true))
            }
            Action::Rename {
                entry,
                dir,
                name: n,
                mode,
            } => match pick(&entries, entry) {
                Some((p, old, _, _)) => {
                    let mode = [
                        RenameMode::Unspecified,
                        RenameMode::Noreplace,
                        RenameMode::Exchange,
                    ][usize::from(mode)];
                    h.rename(p, &old, pick(&dirs, dir).unwrap(), name(n), mode)
                }
                None => Ok(()),
            },
            Action::Link(f, d, n) => {
                pick(&files, f).map_or(Ok(()), |f| h.link(f, pick(&dirs, d).unwrap(), name(n)))
            }
            Action::Fallocate {
                file,
                off,
                len,
                mode,
            } => match pick(&files, file) {
                Some(f) => {
                    let mode = FallocateMode::try_from(i32::from(mode)).unwrap();
                    h.fallocate(f, u64::from(off), u64::from(len), mode)
                }
                None => Ok(()),
            },
            Action::Clone {
                src,
                dst,
                so,
                d_off,
                len,
            } => match (pick(&files, src), pick(&files, dst)) {
                (Some(s), Some(d)) => {
                    h.clone_range(s, u64::from(so), d, u64::from(d_off), u64::from(len))
                }
                _ => Ok(()),
            },
            Action::Xattr(f, n, set) => match pick(&files, f) {
                Some(f) if set => h.set_xattr(
                    f,
                    &format!("user.{}", name(n)),
                    &[n],
                    XattrMode::Unspecified,
                ),
                Some(f) => h.remove_xattr(f, &format!("user.{}", name(n))),
                None => Ok(()),
            },
            Action::Seal => {
                h.seal();
                Ok(())
            }
            Action::CloseAll => {
                h.close_all();
                Ok(())
            }
            Action::Remount => {
                h.remount();
                Ok(())
            }
        };
    }
    h.check();
}

proptest! {
    #![proptest_config(ProptestConfig { cases: 256, ..ProptestConfig::default() })]

    #[test]
    fn live_equals_replay(actions in prop::collection::vec(action(), 1..80)) {
        run(&actions);
    }
}
