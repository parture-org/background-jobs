use std::time::Duration;

use actix::{Actor, Addr, AsyncContext, Context, Handler, SyncContext};

use crate::{CheckDb, ProcessJob, Server};

pub struct Pinger<W>
where
    W: Actor + Handler<ProcessJob>,
{
    server: Addr<Server<W>>,
}

impl<W> Pinger<W>
where
    W: Actor + Handler<ProcessJob>,
{
    pub fn new(server: Addr<Server<W>>) -> Self {
        Pinger { server }
    }
}

impl<W> Actor for Pinger<W>
where
    W: Actor + Handler<ProcessJob>,
    Server<W>: Actor<Context = SyncContext<Server<W>>> + Handler<CheckDb>,
{
    type Context = Context<Self>;

    fn started(&mut self, ctx: &mut Self::Context) {
        ctx.run_interval(Duration::from_secs(1), |actor, _| {
            actor.server.do_send(CheckDb);
        });
    }
}
