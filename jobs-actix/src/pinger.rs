use std::time::Duration;

use actix::{Actor, Addr, AsyncContext, Context, Handler, SyncContext};
use background_jobs_core::Storage;

use crate::{CheckDb, ProcessJob, Server};

pub struct Pinger<S, W>
where
    S: Storage + 'static,
    W: Actor + Handler<ProcessJob>,
{
    server: Addr<Server<S, W>>,
}

impl<S, W> Pinger<S, W>
where
    S: Storage + 'static,
    W: Actor + Handler<ProcessJob>,
{
    pub fn new(server: Addr<Server<S, W>>) -> Self {
        Pinger { server }
    }
}

impl<S, W> Actor for Pinger<S, W>
where
    S: Storage + 'static,
    W: Actor + Handler<ProcessJob>,
    Server<S, W>: Actor<Context = SyncContext<Server<S, W>>> + Handler<CheckDb>,
{
    type Context = Context<Self>;

    fn started(&mut self, ctx: &mut Self::Context) {
        ctx.run_interval(Duration::from_secs(1), |actor, _| {
            actor.server.do_send(CheckDb);
        });
    }
}
