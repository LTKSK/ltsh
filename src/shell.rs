use crate::helper::DynError;
use nix::{
    libc,
    sys::{
        signal::{killpg, signal, SigHandler, Signal},
        wait::{waitpid, WaitPidFlag, WaitStatus},
    },
    unistd::{self, dup2, execvp, fork, pipe, setpgid, tcgetpgrp, tcsetpgrp, ForkResult, Pid},
};
use rustyline::{error::ReadlineError, Editor};
use signal_hook::{consts::*, iterator::Signals};
use std::{
    collections::{BTreeMap, HashMap, HashSet},
    ffi::CString,
    mem::replace,
    path::PathBuf,
    process::exit,
    sync::mpsc::{channel, sync_channel, Receiver, SendError, SyncSender},
    thread,
};

// ラッパー。EINTRならリトライする
fn syscall<F, T>(f: F) -> Result<T, nix::Error>
where
    F: Fn() -> Result<T, nix::Error>,
{
    loop {
        match f() {
            Err(nix::Error::EINTR) => (),
            result => return result,
        }
    }
}

enum WorkerMsg {
    Signal(i32),
    Cmd(String), // コマンドの入力
}

enum ShellMsg {
    Continue(i32), // shellの読み込みを再開。値は最後の終了コード
    Quit(i32),     // shellを終了する。値は終了コード
}

#[derive(Debug)]
pub struct Shell {
    logfile: String,
}

impl Shell {
    pub fn new(logfile: &str) -> Self {
        Shell {
            logfile: logfile.to_string(),
        }
    }

    // main thread
    pub fn run(&self) -> Result<(), DynError> {
        // SIGTTOUを無視しないとSIGTSTPが送信されてしまうらしい
        unsafe { signal(Signal::SIGTTOU, SigHandler::SigIgn).unwrap() };

        // Editorで標準入力を読み込んでいる
        let mut r1 = Editor::<()>::new()?; //
                                           // 読み込んだらまずはヒストリファイルを読み込んでいる
        if let Err(e) = r1.load_history(&self.logfile) {
            eprintln!("ltsh: ヒストリファイルの読み込みに失敗: {e}");
        }

        // workerスレッドとshellスレッド用
        let (worker_tx, worker_rx) = channel();
        // 0を入れると同期的なチャンネルになるんだとか
        let (shell_tx, shell_rx) = sync_channel(0);
        spawn_sig_handler(worker_tx.clone())?;

        Worker::new().spawn(worker_rx, shell_tx);

        let exit_val; // 終了コード
        let mut prev = 0; // 直前のプロセスの終了コード
        loop {
            let face = if prev == 0 { '\u{1F642}' } else { '\u{1F480}' };
            match r1.readline(&format!("ltsh {face} %>")) {
                Ok(line) => {
                    let line_trimed = line.trim();
                    if line_trimed.is_empty() {
                        continue; // からの場合再読込
                    } else {
                        r1.add_history_entry(line_trimed);
                    }

                    // 読み込んだ1行をワーカーに送信
                    worker_tx.send(WorkerMsg::Cmd(line)).unwrap();
                    // ここで受信待ち。recvが待受する関数
                    match shell_rx.recv().unwrap() {
                        ShellMsg::Continue(n) => prev = n, // 読み込み再開
                        ShellMsg::Quit(n) => {
                            exit_val = n;
                            break;
                        }
                    }
                }
                Err(ReadlineError::Interrupted) => eprintln!("ltsh: 終了は Ctrl-d"),
                Err(ReadlineError::Eof) => {
                    worker_tx.send(WorkerMsg::Cmd("exit".to_string())).unwrap();
                    match shell_rx.recv().unwrap() {
                        ShellMsg::Quit(n) => {
                            exit_val = n;
                            break;
                        }
                        _ => panic!("exit失敗"),
                    }
                }
                Err(e) => {
                    eprintln!("ltsh: 読み込みエラー\n{e}");
                    exit_val = 1;
                    break;
                }
            }
        }
        if let Err(e) = r1.save_history(&self.logfile) {
            eprintln!("ltsh: ヒストリファイルへの書き込みに失敗: {e}");
        }

        exit(exit_val);
    }
}
