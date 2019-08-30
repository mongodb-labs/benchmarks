#!/usr/bin/env sysbench

-- Test the effect of opening lots of connections.

-- Each thread opens a connection as usual and executes a busy workload. In addition all or some
-- threads open lots of mostly-idle connections that are held in an array and pinged every
-- --idle-interval. One event (loop) opens one connection and these contribute to events/sec and
-- latency, but are typically a minority of events.

-- Options: --threads --idle-handlers --idle-connections

-- To always maintain max load, select options such that
--    threads - idle_handlers >= cores on client system

-- In an ec2 placement_group cluster, we observed ~3k new connections / thread / minute. (52/sec)
-- To open connections faster (thundering herd test) increase --idle-handlers.
-- Standard option --rate can also be useful.

-- TODO: allow to specify a delay before opening --idle-connections.

require("common")
local MongoClient = require("mongorover.MongoClient")


ffi.cdef("int sleep(int seconds);")
ffi.cdef[[
void sb_counter_inc(int thread_id, sb_counter_type type);
]]


-- Options specific to this test
sysbench.cmdline.options["idle-connections"] = {"How many idle connections to create.", 100}
sysbench.cmdline.options["idle-interval"] = {"Seconds after which idle connections do a single find().", 300}
sysbench.cmdline.options["idle-handlers"] = {"How many threads participate in opening idle connections. 0 = all of them.", 0}


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
    assert( sysbench.opt.idle_handlers <= sysbench.opt.threads, "--idle-handlers cannot be higher than --threads.")

    my_idle_handlers = sysbench.opt.idle_handlers
    if my_idle_handlers == 0 then
        my_idle_handlers = sysbench.opt.threads
    end
    my_num_idle_connections = math.floor( sysbench.opt.idle_connections / my_idle_handlers )
    -- First thread takes care of the remainder
    my_num_idle_connections = my_num_idle_connections + sysbench.opt.idle_connections % my_num_idle_connections
    if thread_id >= my_idle_handlers then
        my_num_idle_connections = 0
    end
    idle_timer = os.time() + sysbench.opt.idle_interval
end

function thread_done(thread_id)
end

local toggle = 0
function event(thread_id)
    if toggle == 0 then
        idle_event(thread_id)
    else
        busy_event(thread_id)
    end
    toggle = (toggle + 1) % 2
end


-- Like getDB but creates a new connection each time
function new_connection()
    local client = MongoClient.new(sysbench.opt.mongo_url)
    ffi.C.sb_counter_inc(sysbench.tid, ffi.C.SB_CNT_RECONNECT)
    return client:getDatabase(sysbench.opt.db_name)
end

function new_getCollection ()
    local db = new_connection()
    return db:getCollection(sysbench.opt.collection_name)
end

local idle_connections_created = false
local idle_conns = {}
local next_idle_index = 1
local idle_conn_idx = 1
function create_idle_connections(thread_id)
    if next_idle_index <= my_num_idle_connections then
        idle_conns[next_idle_index] = new_getCollection()
        next_idle_index = next_idle_index + 1
        -- Simulate real network latency.
        -- usleep(sysbench.rand.uniform(50, 150)*4000)
    else
        idle_connections_created = true
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
    do_query(getCollection())
end

function idle_event(thread_id)
    -- When all connections are created, this does nothing
    create_idle_connections(thread_id)

    -- If it's time to ping the idle connections, do so, one by one. Otherwise, just act like a regular thread.
    if idle_connections_created and idle_timer < os.time() then
        do_query(idle_conns[idle_conn_idx])
        idle_conn_idx = idle_conn_idx + 1
        if idle_conn_idx >= my_num_idle_connections then
            -- Done pinging idle connections. Leave them alone until next interval.
            idle_conn_idx = 1
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
    -- my_results contains the results that are supposed to be "interesting" for this
    -- benchmark. The key names should be useful as lables.
    my_results = {}
    my_results["latency_" .. sysbench.opt.percentile .. "_ms"] = stat.latency_pct * 1000
    my_results["ops_per_sec"] = stat.reads / stat.time_total
    my_results["new_connections_total"] = stat.reconnects
    common_report_cumulative(stat, my_results)
end
