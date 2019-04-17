#include "src/main/cc/proxy_client/javac_remote_actions.h"

#include <algorithm>
#include <iostream>
#include <set>
#include <string>
#include <sys/stat.h>

#include "absl/strings/ascii.h"
#include "absl/strings/str_split.h"

namespace remote_client {

const int kMaxArgsToCheckForJavac = 8;

const char* kJavacWrapperCommand = "javac_wrapper ";
const char* kJavacCommand = "javac ";

const char* kJavacWithOptionsCommand = "javac -";

using std::min;
using std::string;
using std::set;
using std::cout;

string Trim(const string& cmd) {
  if (cmd.length() == 0) {
    return "";
  }

  size_t st_idx = 0;
  size_t ed_idx = cmd.length() - 1;

  while (st_idx <= ed_idx && !absl::ascii_isalpha(cmd[st_idx])) ++st_idx;
  while (ed_idx > st_idx && !absl::ascii_isalpha(cmd[ed_idx])) --ed_idx;

  return cmd.substr(st_idx, ed_idx - st_idx + 1);
}

bool IsFile(const string& name) {
  struct stat buffer;   
  return stat(name.c_str(), &buffer) == 0;
}

void FindFiles(const string& cmd, set<string>* files) {
  if (cmd.find(" ") != string::npos) {
    for (const auto& c : absl::StrSplit(cmd, ' ', absl::SkipEmpty())) {
      FindFiles(string(c), files);
    }
    return;
  }
  if (cmd.find(":") != string::npos) {
    for (const auto& c : absl::StrSplit(cmd, ':', absl::SkipEmpty())) {
      FindFiles(string(c), files);
    }
    return;
  }

  const string trimmed_cmd = Trim(cmd);
  if (IsFile(trimmed_cmd)) {
    files->insert(trimmed_cmd);
  }
}

void FindAllFilesFromCommand(int argc, char** argv, set<string>* files) {
  if (files == nullptr) {
    return;
  }

  for (int i = 0; i < argc; ++i) {
    FindFiles(string(argv[i]), files);
  }
}

bool IsJavacAction(int argc, char** argv) {
  const int num_args_to_check = min(kMaxArgsToCheckForJavac, argc);

  for (int i = 0; i < num_args_to_check; ++i) {
    const string cur_arg(argv[i]);
    if ((cur_arg.find(kJavacWrapperCommand) != string::npos &&
        cur_arg.find(kJavacCommand) != string::npos) ||
        cur_arg.find(kJavacWithOptionsCommand) != string::npos) {
      return true;
    }
  }
  return false;
}

} // namespace remote_client
