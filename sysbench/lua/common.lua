local ffi = require('ffi')
pretty = require 'pl.pretty'
local MongoClient = require("mongorover.MongoClient")
local JSON = require("JSON")

sysbench.cmdline.options = {
    -- the default values for built-in options are currently ignored, see 
    -- https://github.com/akopytov/sysbench/issues/151
    ["mongo-url"] = {"Mongo Client connector string", "mongodb://localhost:27017/"},
    ["db-name"] = {"Database name", "sbtest"},
    ["collection-name"] = {"Collection name", "sbtest"},
    ["num-docs"] = {"How many documents in a collection", 1000},
    ["batch-size"] = {"batch insert size", 1000},
    ["csv-file"] = {"File to report statistics when --report-interval is set (0 to print to stdout)", "sysbench.csv"},
    ["verbose"] = {"verbosity", 0}
}
-- Add your own options in the benchmark file like:
-- sysbench.cmdline.options["opt"] = {"Description", "default value"}

-- Without the global, new connection created for each query
local static_db = nil
function getDB()
    if not static_db then
        local client = MongoClient.new(sysbench.opt.mongo_url)
        static_db = client:getDatabase(sysbench.opt.db_name)
    end
    return static_db
end

function getCollection (n)
    n = tostring(n or "")
    local db = getDB()
    local collection_name = sysbench.opt.collection_name .. n
    return db:getCollection(collection_name)
end

local csv_headers = "time,threads,events,ops,reads,writes,other,latency_ms,errors,reconnects"
function format_csv(stat)
    local seconds = stat.time_interval
    return string.format("%.0f,%u,%4.2f,%4.2f,%4.2f,%4.2f,%4.2f,%4.2f,%4.2f,%4.2f",
           stat.time_total, stat.threads_running,
           stat.events / seconds, (stat.reads + stat.writes + stat.other) / seconds,
           stat.reads / seconds, stat.writes / seconds, stat.other / seconds,
           stat.latency_pct * 1000, stat.errors / seconds, stat.reconnects / seconds)
end

local file = nil
local print_once_memory = false
function common_report_intermediate(stat)
    if sysbench.opt.csv_file then
        if not print_once_memory then
            file = io.open(sysbench.opt.csv_file, "w")
            file:write(csv_headers .. "\n")
            print("Printing statistics to " .. sysbench.opt.csv_file)
            print_once_memory = true
        end
        file:write(format_csv(stat) .. "\n")
    else
        if not print_once_memory then
            print(csv_headers .. "\n")
            print_once_memory = true
        end
        print(format_csv(stat) .. "\n")
    end
end

-- stat is the object provided by sysbench. 
-- my_results should contain the selection of results that are relevant for this specific test.
function common_report_cumulative(stat, my_results)
    -- Dump options. For example, we want to see the nr of threads that was used.
    print("--- sysbench json options start ---")
    print(JSON:encode(sysbench.opt))
    print("--- sysbench json options end ---")

    -- Dum raw stats.
    print("--- sysbench json stats start ---")
    print(JSON:encode(stat))
    print("--- sysbench json stats end ---")

    -- The "results" section contains the results that are supposed to be "interesting" for this
    -- benchmark. The key names should be useful as lables.
    print("--- sysbench json results start ---")
    print(JSON:encode(my_results))
    print("--- sysbench json results end ---")
end


-- standard function library :-)
ffi.cdef("int usleep(int microseconds);")
function usleep(microseconds)
    ffi.C.usleep(microseconds)
end