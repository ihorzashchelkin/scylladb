#include <boost/algorithm/string.hpp>
#include <filesystem>
#include <set>
#include <fmt/chrono.h>
#include <fmt/ostream.h>
#include <fmt/ranges.h>
#include <seastar/core/coroutine.hh>
#include <seastar/core/queue.hh>
#include <seastar/core/units.hh>
#include <seastar/http/short_streams.hh>
#include <seastar/util/closeable.hh>
#include <seastar/core/queue.hh>

#include "db/commitlog/commitlog_entry.hh"
#include "db/commitlog/commitlog_replayer.hh"
#include "init.hh"
#include "compaction/compaction.hh"
#include "compaction/compaction_strategy.hh"
#include "compaction/compaction_strategy_state.hh"
#include "cql3/type_json.hh"
#include "cql3/statements/raw/parsed_statement.hh"
#include "cql3/statements/select_statement.hh"
#include "db/config.hh"
#include "db/large_data_handler.hh"
#include "db/corrupt_data_handler.hh"
#include "gms/feature_service.hh"
#include "reader_concurrency_semaphore.hh"
#include "readers/combined.hh"
#include "readers/generating.hh"
#include "schema/schema_builder.hh"
#include "seastar/core/future.hh"
#include "sstables/index_reader.hh"
#include "sstables/sstables_manager.hh"
#include "sstables/sstable_directory.hh"
#include "sstables/open_info.hh"
#include "replica/schema_describe_helper.hh"
#include "test/lib/cql_test_env.hh"
#include "tools/json_writer.hh"
#include "tools/load_system_tablets.hh"
#include "tools/lua_sstable_consumer.hh"
#include "tools/schema_loader.hh"
#include "tools/sstable_consumer.hh"
#include "tools/utils.hh"
#include "locator/host_id.hh"
#include "db/commitlog/commitlog.hh"
#include "db/commitlog/commitlog_types.hh"
#include "db/commitlog/commitlog_entry.hh"

using namespace seastar;

using json_writer = mutation_json::json_writer;
using commitlog = db::commitlog;

namespace bpo = boost::program_options;

using namespace tools::utils;

using operation_func = void (*)();

namespace {

const auto app_name = "commitlog";

logging::logger clt_log(format("scylla-{}", app_name));

const std::vector<operation_option> global_options{};

const std::vector<operation_option> global_positional_options{};

const std::map<operation, operation_func> operations_with_func{
        /* dump */
        {{"dump", "TODO", "TODO", {}},
                []() {
                    //
                }},
};

} // anonymous namespace

namespace tools {

int scylla_commitlog_main(int argc, char** argv) {
    const auto description = "";

    const auto operations = operations_with_func | std::views::keys | std::ranges::to<std::vector>();
    tool_app_template::config app_cfg{.name = app_name,
            .description = description,
            .logger_name = clt_log.name(),
            .lsa_segment_pool_backend_size_mb = 100,
            .operations = std::move(operations),
            .global_options = &global_options,
            .global_positional_options = &global_positional_options,
            .db_cfg_ext = db_config_and_extensions()};
    tool_app_template app(std::move(app_cfg));

    return app.run_async(argc, argv, [&app](const operation& operation, const bpo::variables_map& app_config) -> int {
        auto& _ = app;

#if 0
        auto& dbcfg = *app.cfg().db_cfg_ext->db_cfg;

        sstring scylla_yaml_path;
        sstring scylla_yaml_path_source;

        dbcfg.enable_cache(false);
        dbcfg.volatile_system_keyspace_for_testing(true);

        distributed<replica::database> db;
        sharded<cql3::query_processor> qp;

        static sharded<db::system_keyspace> sys_ks;
        sys_ks.start(std::ref(qp), std::ref(db)).get();

        auto rp = db::commitlog_replayer::create_replayer(db, sys_ks).get();

        rp.recover(sstring file, sstring fname_prefix);

        commitlog_entry_reader cer{};

        auto d = commitlog::descriptor("/var/lib/scylla/commitlog/XXX", db::commitlog::descriptor::FILENAME_PREFIX);
#endif
        auto filename = "/var/lib/scylla/commitlog/CommitLog-4-108086391147835812.log";
        commitlog::read_log_file(filename, db::commitlog::descriptor::FILENAME_PREFIX, [](commitlog::buffer_and_replay_position buf_rp) -> future<> {
            auto&& buf = buf_rp.buffer;
            auto&& rp = buf_rp.position;

            commitlog_entry_reader cer(buf);
            auto& fm = cer.mutation();

            json_writer;

            return make_ready_future();
        }).get();

        try {
            operations_with_func.at(operation)();
        } catch (std::invalid_argument& e) {
            fmt::print(std::cerr, "error processing arguments: {}\n", e.what());
            return 1;
        } catch (...) {
            fmt::print(std::cerr, "error running operation: {}\n", std::current_exception());
            return 2;
        }

        return 0;
    });
}

} // namespace tools
