/*
  __  __                         ____  ____
 |  \/  | ___  _ __   __ _  ___ |  _ \| __ )
 | |\/| |/ _ \| '_ \ / _` |/ _ \| | | |  _ \
 | |  | | (_) | | | | (_| | (_) | |_| | |_) |
 |_|  |_|\___/|_| |_|\__, |\___/|____/|____/
                     |___/
A filter plugin that logs every message it receives to MongoDB, one collection
per topic, with the same schema the former `mads-logger` agent used:

  JSON message: { timestamp: <date, reception time>, message: <payload> }
  blob message: { timestamp: <date>, message: <payload>, data: <binary> }
  bad payload:  { timestamp: <date>, error: <conversion error> }

A `metadata` message with a boolean `pause` field pauses/resumes logging.
Every `status_period_ms` (default 1000, 0 disables) the plugin publishes
`{"logger_paused": <bool>}` on the `logger_status` topic: run it with
`dont_block = true`, or the status goes out only when messages arrive. On
shutdown, every collection written gets indexes on message.timestamp,
message.hostname and message.timecode.

Author(s): Paolo Bosetti
*/

#include <nlohmann/json.hpp>
#include <pugg/Kernel.h>
#include <filter.hpp>

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <iostream>
#include <set>
#include <string>

#include "mongo_instance.hpp"
#include <bsoncxx/builder/basic/array.hpp>
#include <bsoncxx/builder/basic/document.hpp>
#include <bsoncxx/json.hpp>
#include <bsoncxx/types.hpp>
#include <mongocxx/client.hpp>
#include <mongocxx/database.hpp>
#include <mongocxx/uri.hpp>

#ifndef PLUGIN_NAME
#define PLUGIN_NAME "mongodb"
#endif

using namespace std;
using json = nlohmann::json;
using bsoncxx::builder::basic::kvp;
using bsoncxx::builder::basic::make_document;
using bsoncxx::types::b_date;

/*
  nlohmann::json -> BSON, extended-JSON aware for "$date". Byte-identical to
  bsoncxx::from_json() for the JSON shapes MADS produces (checked in main()),
  without the dump + re-parse round trip.
*/
namespace {
using bsoncxx::builder::basic::sub_array;
using bsoncxx::builder::basic::sub_document;

// Parse "YYYY-MM-DDThh:mm:ss[.fff][Z|±hh[:]mm]" to ms since the Unix epoch.
bool parse_iso8601_ms(const string &s, int64_t &out) {
  int Y, Mo, D, h, mi, se;
  if (sscanf(s.c_str(), "%d-%d-%dT%d:%d:%d", &Y, &Mo, &D, &h, &mi, &se) != 6)
    return false;
  int ms = 0;
  if (auto dot = s.find('.'); dot != string::npos) {
    string f;
    for (size_t i = dot + 1;
         i < s.size() && isdigit((unsigned char)s[i]) && f.size() < 3; ++i)
      f += s[i];
    while (f.size() < 3)
      f += '0';
    ms = stoi(f);
  }
  int tz = 0; // minutes east of UTC
  auto tpos = s.find('T');
  string tail = (tpos == string::npos) ? s : s.substr(tpos + 1);
  if (tail.find('Z') == string::npos) {
    auto p = tail.find_last_of("+-");
    if (p != string::npos) {
      int sign = tail[p] == '-' ? -1 : 1;
      string off = tail.substr(p + 1);
      int oh = 0, om = 0;
      if (off.find(':') != string::npos)
        sscanf(off.c_str(), "%d:%d", &oh, &om);
      else if (off.size() >= 4) {
        oh = stoi(off.substr(0, 2));
        om = stoi(off.substr(2, 2));
      } else if (!off.empty())
        oh = stoi(off);
      tz = sign * (oh * 60 + om);
    }
  }
  using namespace std::chrono;
  sys_days days = year{Y} / Mo / D;
  auto tp = days + hours{h} + minutes{mi} + seconds{se} + milliseconds{ms} -
            minutes{tz};
  out = duration_cast<milliseconds>(tp.time_since_epoch()).count();
  return true;
}

bool date_millis(const json &v, int64_t &out) {
  if (v.is_string())
    return parse_iso8601_ms(v.get<string>(), out);
  if (v.is_number_integer() || v.is_number_unsigned()) {
    out = v.get<int64_t>();
    return true;
  }
  if (v.is_object()) {
    auto it = v.find("$numberLong");
    if (it != v.end() && it->is_string()) {
      out = stoll(it->get<string>());
      return true;
    }
  }
  return false;
}

bool is_date_obj(const json &v, int64_t &ms) {
  if (!v.is_object() || v.size() != 1)
    return false;
  auto it = v.find("$date");
  return it != v.end() && date_millis(*it, ms);
}

void append_array(sub_array arr, const json &v);

void append_value(sub_document doc, const string &key, const json &v) {
  using namespace bsoncxx::types;
  int64_t ms;
  switch (v.type()) {
  case json::value_t::object:
    if (is_date_obj(v, ms)) {
      doc.append(kvp(key, b_date{chrono::milliseconds{ms}}));
      break;
    }
    doc.append(kvp(key, [&](sub_document sub) {
      for (auto &el : v.items())
        append_value(sub, el.key(), el.value());
    }));
    break;
  case json::value_t::array:
    doc.append(kvp(key, [&](sub_array sub) {
      for (auto &el : v)
        append_array(sub, el);
    }));
    break;
  case json::value_t::string:
    doc.append(kvp(key, v.get<string>()));
    break;
  case json::value_t::boolean:
    doc.append(kvp(key, v.get<bool>()));
    break;
  case json::value_t::number_integer:
  case json::value_t::number_unsigned: {
    int64_t n = v.get<int64_t>();
    if (n >= INT32_MIN && n <= INT32_MAX)
      doc.append(kvp(key, b_int32{(int32_t)n}));
    else
      doc.append(kvp(key, b_int64{n}));
    break;
  }
  case json::value_t::number_float:
    doc.append(kvp(key, v.get<double>()));
    break;
  default:
    doc.append(kvp(key, b_null{})); // null / discarded / binary
    break;
  }
}

void append_array(sub_array arr, const json &v) {
  using namespace bsoncxx::types;
  int64_t ms;
  switch (v.type()) {
  case json::value_t::object:
    if (is_date_obj(v, ms)) {
      arr.append(b_date{chrono::milliseconds{ms}});
      break;
    }
    arr.append([&](sub_document sub) {
      for (auto &el : v.items())
        append_value(sub, el.key(), el.value());
    });
    break;
  case json::value_t::array:
    arr.append([&](sub_array sub) {
      for (auto &el : v)
        append_array(sub, el);
    });
    break;
  case json::value_t::string:
    arr.append(v.get<string>());
    break;
  case json::value_t::boolean:
    arr.append(v.get<bool>());
    break;
  case json::value_t::number_integer:
  case json::value_t::number_unsigned: {
    int64_t n = v.get<int64_t>();
    if (n >= INT32_MIN && n <= INT32_MAX)
      arr.append(b_int32{(int32_t)n});
    else
      arr.append(b_int64{n});
    break;
  }
  case json::value_t::number_float:
    arr.append(v.get<double>());
    break;
  default:
    arr.append(b_null{});
    break;
  }
}

bsoncxx::document::value json_to_bson(const json &j) {
  bsoncxx::builder::basic::document doc;
  if (j.is_object())
    for (auto &el : j.items())
      append_value(doc, el.key(), el.value());
  return doc.extract();
}
} // namespace

class MongodbPlugin : public Filter<json, json> {

public:
  using Filter::Filter;

  ~MongodbPlugin() {
    log_own_shutdown();
    create_indexes();
  }

  string kind() override { return PLUGIN_NAME; }

  return_type load_data(json const &input, string topic = "",
                        vector<unsigned char> const *blob = nullptr) override {
    if (!_connected) {
      return return_type::critical; // _error set by set_params()
    }
    _got_input = true;
    // Always success, logged or not: process() must run to publish the status
    if (topic == "metadata" && input.contains("pause") &&
        input["pause"].is_boolean()) {
      _paused = input["pause"].get<bool>();
      cout << (_paused ? "\nLogging is paused" : "\nLogging is resumed")
           << endl;
    }
    // logger_status: our own status (and other loggers'), never logged
    if (_paused || topic.empty() || topic == "logger_status") {
      return return_type::success;
    }

    auto now = chrono::system_clock::now();
    auto doc = make_document();
    try {
      if (blob) {
        bsoncxx::types::b_binary bin{bsoncxx::binary_sub_type::k_binary,
                                     static_cast<uint32_t>(blob->size()),
                                     blob->data()};
        doc = make_document(kvp("timestamp", b_date(now)),
                            kvp("message", json_to_bson(input)),
                            kvp("data", bin));
      } else {
        doc = make_document(kvp("timestamp", b_date(now)),
                            kvp("message", json_to_bson(input)));
      }
    } catch (const exception &e) {
      doc = make_document(kvp("timestamp", b_date(now)),
                          kvp("error", e.what()));
    }
    try {
      _db[topic].insert_one(doc.view());
      _topics.insert(topic);
    } catch (const exception &e) {
      // Not return_type::error: the host would publish an event for it,
      // which this plugin receives and fails to store again, in a loop.
      cerr << "\nError while inserting document: " << e.what() << endl;
    }
    return return_type::success;
  }

  // Publishes the status when due; otherwise nothing.
  return_type process(json &out, vector<unsigned char> * = nullptr) override {
    auto now = chrono::steady_clock::now();
    bool busy = _got_input;
    _got_input = false;
    if (_status_period > 0ms && now >= _next_status) {
      out = {{"logger_paused", _paused}, {"topic", "logger_status"}};
      _next_status = now + _status_period;
      next_loop_duration = 0ms;
      return return_type::success;
    }
    // Idle (dont_block): wait a little, not until the next status, so that
    // the reception timestamp of the next message stays accurate.
    // 0 means "use the period", i.e. free run while messages keep coming.
    auto wait = _status_period > 0ms
                    ? chrono::duration_cast<chrono::milliseconds>(_next_status - now)
                    : kIdleWait;
    next_loop_duration = busy ? 0ms : clamp(wait, 1ms, kIdleWait);
    return return_type::retry;
  }

  void set_params(const json &params) override {
    Filter::set_params(params);
    _params["mongo_uri"] = "mongodb://localhost:27017";
    _params["mongo_db"] = "mads";
    _params["initially_paused"] = false;
    _params["status_period_ms"] = 1000;
    _params.merge_patch(params);
    _paused = _params["initially_paused"].get<bool>();
    _status_period =
        chrono::milliseconds(max<int64_t>(0, _params["status_period_ms"].get<int64_t>()));
    _next_status = chrono::steady_clock::now();
    try {
      Mads::detail::mongo_instance();
      _client =
          mongocxx::client{mongocxx::uri{_params["mongo_uri"].get<string>()}};
      _db = _client[_params["mongo_db"].get<string>()];
      _connected = true;
    } catch (const exception &e) {
      _error = string("Cannot open MongoDB: ") + e.what();
      _connected = false;
    }
  }

  map<string, string> info() override {
    return {{"MongoDB URI", _params.value("mongo_uri", "")},
            {"MongoDB database", _params.value("mongo_db", "")},
            {"Initially paused", _paused ? "yes" : "no"},
            {"Status period",
             _status_period > 0ms
                 ? to_string(_status_period.count()) + " ms" +
                       (_params.value("dont_block", false)
                            ? ""
                            : " (only on traffic: set dont_block = true)")
                 : "disabled"}};
  }

private:
  // The host publishes its shutdown event after the last load_data(), so it
  // never comes back to this sink: store it directly, as mads-logger did.
  // Must not throw: called by the destructor.
  void log_own_shutdown() noexcept {
    if (!_connected)
      return;
    try {
      json payload = {{"name", _params.value("agent_name", PLUGIN_NAME)},
                      {"event", "shutdown"},
                      {"settings", _params}};
      _db["agent_event"].insert_one(
          make_document(kvp("timestamp", b_date(chrono::system_clock::now())),
                        kvp("message", json_to_bson(payload))));
      _topics.insert("agent_event");
    } catch (const exception &e) {
      cerr << "Error while logging the shutdown event: " << e.what() << endl;
    }
  }

  // Must not throw: called by the destructor
  void create_indexes() noexcept {
    if (!_connected)
      return;
    try {
      for (auto &t : _topics) {
        _db[t].create_index(make_document(kvp("message.timestamp", 1)));
        _db[t].create_index(make_document(kvp("message.hostname", 1)));
        _db[t].create_index(make_document(kvp("message.timecode", 1)));
      }
    } catch (const exception &e) {
      cerr << "Error while creating indexes: " << e.what() << endl;
    }
  }

  static constexpr chrono::milliseconds kIdleWait{10};

  mongocxx::client _client;
  mongocxx::database _db;
  set<string> _topics;
  chrono::milliseconds _status_period{1000};
  chrono::steady_clock::time_point _next_status;
  bool _paused = false;
  bool _connected = false;
  bool _got_input = false;
};

MADS_REGISTER_PLUGINS(MongodbPlugin)

/*
  Test driver. Always checks json_to_bson() against bsoncxx::from_json(); with
  MONGO_TEST_URI set, also writes to the mads_plugin_test database and reads
  the documents back (run ./replay afterwards to replay them).
*/
#define CHECK(cond)                                                            \
  do {                                                                         \
    if (!(cond)) {                                                             \
      cerr << "FAIL " << __LINE__ << ": " #cond << endl;                       \
      return 1;                                                                \
    }                                                                          \
  } while (0)

int main() {
  // 1. Conversion parity with the driver's own JSON parser
  const char *samples[] = {
      R"({"a":1,"b":-2147483649,"c":3.5,"d":"x","e":true,"f":null,)"
      R"("g":[1,"y",{"h":[]}],"i":{"j":{"k":false}}})",
      R"({"timestamp":{"$date":"2026-11-24T13:20:06.896Z"}})",
      R"({"t":{"$date":"2026-11-24T14:20:06.896+01:00"},)"
      R"("u":{"$date":"2026-11-24T14:20:06.896+0100"}})",
      R"({"v":[{"$date":{"$numberLong":"1764000000000"}}]})"};
  for (auto s : samples) {
    auto ours = json_to_bson(json::parse(s));
    auto ref = bsoncxx::from_json(s);
    CHECK(ours.view().length() == ref.view().length());
    CHECK(memcmp(ours.view().data(), ref.view().data(), ref.view().length()) ==
          0);
  }
  cout << "json_to_bson parity: ok" << endl;

  // 2. Status publishing (no database needed: the URI is parsed, not contacted)
  {
    MongodbPlugin plugin;
    json out;
    plugin.set_params({{"status_period_ms", 50}});
    CHECK(plugin.process(out) == return_type::success);
    CHECK(out["topic"] == "logger_status" && out["logger_paused"] == false);
    CHECK(plugin.process(out) == return_type::retry);
    CHECK(plugin.next_loop_duration > 0ms && plugin.next_loop_duration <= 10ms);
    CHECK(plugin.load_data({{"pause", true}}, "logger_status") ==
          return_type::success); // never logged, so no DB access
    CHECK(plugin.process(out) == return_type::retry);
    CHECK(plugin.next_loop_duration == 0ms); // busy: free run
    plugin.set_params({{"status_period_ms", 0}});
    out.clear();
    CHECK(plugin.process(out) == return_type::retry && out.empty());
  }
  cout << "status: ok" << endl;

  const char *uri = getenv("MONGO_TEST_URI");
  if (!uri) {
    cout << "MONGO_TEST_URI not set: skipping the live MongoDB test" << endl;
    return 0;
  }

  // 3. Live round trip
  Mads::detail::mongo_instance();
  mongocxx::client c{mongocxx::uri{uri}};
  auto db = c["mads_plugin_test"];
  db.drop();
  {
    MongodbPlugin plugin;
    plugin.set_params({{"mongo_uri", uri}, {"mongo_db", "mads_plugin_test"}});
    vector<unsigned char> blob{1, 2, 3, 4};
    plugin.load_data({{"value", 1}}, "test_a");
    plugin.load_data({{"pause", true}}, "metadata");
    plugin.load_data({{"value", 99}}, "test_a"); // paused: not logged
    plugin.load_data({{"pause", false}}, "metadata");
    plugin.load_data({{"value", 2}}, "test_a");
    plugin.load_data({{"value", 3}}, "test_b", &blob);
    plugin.load_data({{"value", 4}}, ""); // no topic: not logged
  } // the destructor creates the indexes

  CHECK(db["test_a"].count_documents({}) == 2);
  CHECK(db["metadata"].count_documents({}) == 1); // the resume message only
  CHECK(db["agent_event"].count_documents(
            make_document(kvp("message.event", "shutdown"))) == 1);
  auto b = db["test_b"].find_one({});
  CHECK(b);
  auto v = b->view();
  CHECK(v["timestamp"].type() == bsoncxx::type::k_date);
  CHECK(v["message"]["value"].get_int32().value == 3);
  CHECK(v["data"].get_binary().size == 4);
  int n_idx = 0;
  for (auto &&i : db["test_a"].list_indexes()) {
    (void)i;
    ++n_idx;
  }
  CHECK(n_idx == 4); // _id + the three message.* ones
  cout << "live MongoDB test: ok" << endl;
  return 0;
}
