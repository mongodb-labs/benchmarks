#!/usr/bin/env sysbench
require("common")

ffi.cdef("int sleep(int seconds);")
ffi.cdef[[
void sb_counter_inc(int thread_id, sb_counter_type type);
]]


-- Options specific to this test
sysbench.cmdline.options["idle-connections"] = {"How many idle connections to create.", 100}
sysbench.cmdline.options["idle-interval"] = {"Seconds after which idle connections do a single find().", 300}


function prepare ()
    local coll = getCollection()

    local arrayOfDocuments = {}
    for i=1, sysbench.opt.num_docs do
        arrayOfDocuments[i] = {_id = i, random = sysbench.rand.uniform(1, 1024)}
    end
    if sysbench.opt.verbose > 0 then
        pretty.dump(arrayOfDocuments)
    end
    local result = coll:insert_many(arrayOfDocuments)

    if sysbench.opt.verbose > 0 then
        pretty.dump(result)
    end
end

function cleanup()
    local coll = getCollection()
    coll:drop()
end

function thread_init(thread_id)
    coll = getCollection()

    if thread_id == 0 then
        idle_conns = {}
        for i = 0, sysbench.opt.idle_connections do
            idle_conns[i] = getCollection()
        end
        -- First queries will happen when 1 second has passed
        idle_timer = os.time()
        idle_conn_idx = 0
    end
end

function thread_done(thread_id)
end

function event(thread_id)
    -- note: thread_id starts from 0
    if thread_id == 0 then
        idle_event()
    else
        busy_event()
    end
end

function do_query(use_this_coll)
    local query = {_id = 0}
    query["_id"] = sysbench.rand.uniform(1, sysbench.opt.num_docs)
    result = use_this_coll:find_one(query)
    if sysbench.opt.verbose > 0 then
        pretty.dump(result)
    end
    ffi.C.sb_counter_inc(sysbench.tid, ffi.C.SB_CNT_READ)
end

function busy_event()
    do_query(coll)
end

function idle_event()
    -- If it's time to ping the idle connections, do so, one by one. Otherwise, just act like a regular thread.
    if idle_timer < os.time() then
        do_query(idle_conns[idle_conn_idx])
        idle_conn_idx = idle_conn_idx + 1
        if idle_conn_idx >= sysbench.opt.idle_connections then
            -- Done pinging idle connections. Leave them alone until next interval.
            idle_conn_idx = 0
            idle_timer = idle_timer + sysbench.opt.idle_interval
        end
    else
        -- When not pinging idle connections, act like a regular thread
        busy_event()
    end
end

function sysbench.hooks.report_intermediate(stat)
    common_report_intermediate(stat)
end

function sysbench.hooks.report_cumulative(stat)
    -- The "results" section contains the results that are supposed to be "interesting" for this
    -- benchmark. The key names should be useful as lables.
    my_results = {}
    my_results["latency_" .. sysbench.opt.percentile .. "_ms"] = stat.latency_pct * 1000
    my_results["ops_per_sec"] = stat.reads / stat.time_total
    common_report_cumulative(stat, my_results)
end
