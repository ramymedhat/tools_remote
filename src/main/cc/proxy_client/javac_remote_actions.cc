#include "src/main/cc/proxy_client/javac_remote_actions.h"

#include <algorithm>

#include "absl/strings/str_split.h"

namespace remote_client {

const int kMaxArgsToCheckForJavac = 8;

const char* kJavacWrapperCommand = "javac_wrapper ";
const char* kJavacCommand = "javac ";

using std::min;

bool IsJavacAction(int argc, char** argv) {
  const int num_args_to_check = min(kMaxArgsToCheckForJavac, argc);

  for (int i = 0; i < num_args_to_check; ++i) {
    const string cur_arg(argv[i]);
    if (cur_arg.find(kJavacWrapperCommand) != string::npos &&
        cur_arg.find(kJavacCommand) != string::npos) {
      return true;
    }
  }
  return false;
}

} // namespace remote_client
