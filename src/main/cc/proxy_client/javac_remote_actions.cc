#include "src/main/cc/proxy_client/javac_remote_actions.h"

#include <algorithm>

#include "absl/strings/str_split.h"

namespace remote_client {

const int kMaxArgsToCheckForJavac = 8;

const char* kJavacWrapperCommand = "javac_wrapper ";
const char* kJavacCommand = "javac ";

using std::min;

void DetermineJavacActionInputs(int argc, char** argv, set<string>* inputs) {
  for (int i = 0; i < argc; ++i) {
    string cur(argv[i]);
    if (cur[0] == '@') {
      inputs->insert(cur.substr(1));
    } else {
      for (const auto& cmd : absl::StrSplit(cur, ' ', absl::SkipEmpty())) {
        if (cmd[0] == '@') {
          inputs->insert(string(cmd.substr(1)));
        }
      }
    }
  }
}

bool IsJavacAction(int argc, char** argv) {
  const int num_args_to_check = min(kMaxArgsToCheckForJavac, argc);

  for (int i = 0; i < num_args_to_check; ++i) {
    const string& cur_arg(argv[i]);
    if (cur_arg.find(kJavacWrapperCommand) != string::npos &&
        cur_arg.find(kJavacCommand) != string::npos) {
      return true;
    }
  }
  return false;
}

} // namespace remote_client
