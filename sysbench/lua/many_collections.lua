#!/usr/bin/env sysbench
require("common")

ffi.cdef[[
void sb_counter_inc(int thread_id, sb_counter_type type);
]]


-- Options specific to this test
sysbench.cmdline.options["parallel-prepare"] = {"Hack: Use 'sysbench many_collections.lua run --parallel-prepare' instead of regular prepare.", false}
sysbench.cmdline.options["loader-sleep"] = {"Loader thread should sleep N seconds before next batch insert.", 0}
sysbench.cmdline.options["num-collections"] = {"How many collections to create. (Each contains num-docs.)", 10}
sysbench.cmdline.options["num-indexes"] = {"Number of secondary indexes, in addition to _id.", 0}
sysbench.cmdline.options["distribution"] = {"uniform or zipfian", "uniform"}

function prepare()
    parallel_prepare(0, 1)
end

local thread_time_keepalive = os.time()
function parallel_prepare (thread_id, num_threads)
    if thread_id > sysbench.opt.num_collections then
        return
    end
    local my_num_collections = math.floor(sysbench.opt.num_collections / num_threads)
    local my_first_collection = thread_id * my_num_collections
    -- last thread takes care of the remainder
    if thread_id == num_threads-1 then
        my_num_collections = my_num_collections + sysbench.opt.num_collections % num_threads
    end
    if sysbench.opt.verbosity >= 3 then
        print("Parallel prepare: thread_id " .. thread_id .. " / " .. num_threads-1 .. " with my_num_collections " .. my_num_collections)
    end
    -- unlike python, lua includes the last parameter to for
    for n=my_first_collection, my_first_collection + my_num_collections - 1 do
        -- print progress every 5 min. Among other things to avoid timeouts.
        if os.time() > thread_time_keepalive + 5*60 or
                sysbench.opt.verbosity >= 4 then

            print("thread_id " .. thread_id .. " collection " .. n)
            thread_time_keepalive = os.time()
        end
        local coll = getCollection(n)
        coll:drop()
        create_indexes(sysbench.opt.collection_name .. n)
        local num_batches = math.floor(sysbench.opt.num_docs / sysbench.opt.batch_size)
        local remainder_start = num_batches * sysbench.opt.batch_size + 1
        local remainder_docs = sysbench.opt.num_docs % sysbench.opt.batch_size
        for b=0, num_batches-1 do
            local start_id = b * sysbench.opt.batch_size + 1
            local stop_id = start_id + sysbench.opt.batch_size - 1
            usleep(sysbench.opt.loader_sleep*1000*1000)
            do_batch(coll, start_id, stop_id)
        end
        if sysbench.opt.verbosity >= 4 then
            print("remainder_start " .. remainder_start .. " remainder_stop " .. remainder_start + remainder_docs - 1)
        end
        do_batch(coll, remainder_start, remainder_start + remainder_docs - 1)
    end
end

function alphabet()
    return "abcdefghijklmnopqrstuvwxyz"
end

function create_indexes(coll_name)
    local db = getDB()
    for i=1,sysbench.opt.num_indexes do
        local field_name = string.sub(alphabet(), i, i)
        if sysbench.opt.verbosity >= 5 then
            print("collection " .. coll_name .. " create_index " .. field_name)
        end
        local index_definition = {}
        index_definition[field_name] = 1
        db:command("createIndexes", coll_name, { indexes = {{ key = index_definition, name = field_name }}})
    end
end

function generate_doc(id)
    return {_id = id, 
            a = get_random(),
            b = get_random(),
            c = get_random(),
            d = get_random(),
            e = get_random(),
            f = get_random(),
            g = get_random(),
            h = get_random(),
            i = get_random(),
            j = get_random(),
            k = get_random(),
            l = get_random(),
            m = get_random(),
            n = get_random(),
            o = get_random(),
            p = get_random(),
            q = get_random(),
            r = get_random(),
            s = get_random(),
            t = get_random(),
            u = get_random(),
            v = get_random(),
            w = get_random(),
            x = get_random(),
            y = get_random(),
            z = get_random(),
    }
end

-- like MAX_INT, but Lua uses doubles
LARGE_INT = 2^30

function get_random(max)
    max = max or LARGE_INT
    if sysbench.opt.distribution == "uniform" then
        return sysbench.rand.uniform(0, max)
    elseif sysbench.opt.distribution == "zipfian" then
        return sysbench.rand.zipfian(0, max)
    end
end

function do_batch(coll, start_id, stop_id)
    if start_id > stop_id then
        return
    end

    local arrayOfDocuments = {}
    for i=start_id, stop_id do
        arrayOfDocuments[#arrayOfDocuments+1] = generate_doc(i)
    end
    if sysbench.opt.verbosity >= 5 then
        pretty.dump(arrayOfDocuments)
    end
    local result = coll:insert_many(arrayOfDocuments)
    if sysbench.opt.verbosity >= 4 then
        pretty.dump(result)
    end
    ffi.C.sb_counter_inc(sysbench.tid, ffi.C.SB_CNT_OTHER)
end

-- End of prepare related functions



function cleanup()
    local db = getDB()
    db:drop_database()
end

local parallel_prepare_done = false
function thread_init(thread_id)
    parallel_prepare_done = false
    thread_time_keepalive = os.time()
end

function thread_done(thread_id)
end

function event(thread_id)
    if sysbench.opt.parallel_prepare then
        if parallel_prepare_done then
            return
        end
        parallel_prepare(thread_id, sysbench.opt.threads)
        parallel_prepare_done = true
        return
    end

    read()
end

function read()
    -- pick _id or a secondary key with equal probability
    local weighted_coin = get_random(100)
    local id_sec_ratio = 100 * 1 / (sysbench.opt.num_indexes + 1)
    if weighted_coin > id_sec_ratio then
        secondary_read()
    else
        id_read()
    end
end

function id_read()
    local coll_nr = get_random(sysbench.opt.num_collections-1)
    local coll = getCollection(coll_nr)
    local id = get_random(sysbench.opt.num_docs-1)
    local result=coll:find_one({_id = id})
    if sysbench.opt.verbosity >= 5 then
        pretty.dump(result)
    end
    ffi.C.sb_counter_inc(sysbench.tid, ffi.C.SB_CNT_READ)
end

function secondary_read()
    -- Secondary fields have random values in a sparse space. Most numbers won't have a record.
    -- We do a range read expected to find on average 1 doc. It's ok that some results will be
    -- empty. The main point is just to traverse the index a bit.
    local coll_nr = get_random(sysbench.opt.num_collections-1)
    local coll = getCollection(coll_nr)
    local range_start = get_random()
    local range_end = range_start + LARGE_INT / sysbench.opt.num_docs
    local i = get_random(26) + 1
    local field_name = string.sub(alphabet(), i, i)
    local query = {}
    local subdoc = {}
    subdoc['$gte'] = range_start
    subdoc['$lte'] = range_end
    query[field_name] = subdoc
    if sysbench.opt.verbosity >= 5 then
        pretty.dump(query)
    end
    local result=coll:find(query)
    if sysbench.opt.verbosity >= 5 then
        pretty.dump(result)
    end
    ffi.C.sb_counter_inc(sysbench.tid, ffi.C.SB_CNT_READ)
end


-- reporting api
function sysbench.hooks.report_intermediate(stat)
    common_report_intermediate(stat)
end

function sysbench.hooks.report_cumulative(stat)
    -- The "results" section contains the results that are supposed to be "interesting" for this
    -- benchmark. The key names should be useful as labels.
    my_results = {}
    if sysbench.opt.parallel_prepare then
        my_results["batches_per_sec"] = stat.other / stat.time_total
    else
        my_results["latency_" .. sysbench.opt.percentile .. "_ms"] = stat.latency_pct * 1000
        my_results["reads_per_sec"] = stat.reads / stat.time_total
        if stat.writes > 0 then
            my_results["writes_per_sec"] = stat.writes / stat.time_total
            my_results["ops_per_sec"] = stat.reads / stat.time_total + stat.writes / stat.time_total
        end
    end

    common_report_cumulative(stat, my_results)
end
