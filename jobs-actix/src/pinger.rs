use actix::{Actor, Addr, AsyncContext, Context};
use std::time::Duration;

use crate::{CheckDb, Server};

pub struct Pinger {
    server: Addr<Server>,
    threads: usize,
}

impl Pinger {
    pub fn new(server: Addr<Server>, threads: usize) -> Self {
        Pinger { server, threads }
    }
}

impl Actor for Pinger {
    type Context = Context<Self>;

    fn started(&mut self, ctx: &mut Self::Context) {
        ctx.run_interval(Duration::from_secs(1), |actor, _| {
            for _ in 0..actor.threads {
                actor.server.do_send(CheckDb);
            }
        });
    }
}
