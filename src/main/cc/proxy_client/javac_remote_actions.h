#ifndef REMOTE_CLIENT_JAVAC_REMOTE_ACTIONS_H_
#define REMOTE_CLIENT_JAVAC_REMOTE_ACTIONS_H_

#include <set>
#include <string>

namespace remote_client {

using std::set;
using std::string;

// IsJavacAction determines if the given action is a java compile action.
bool IsJavacAction(int argc, char** argv);

// FindAllFilesFromCommand splits each of the given args by space and colon,
// checks if each individual piece of command is a file and if it is, appends
// the file to the given set.
// This is useful to find "hidden inputs" i.e., inputs not explicitly specified
// to rbecc (from Ninja), but those that the command depends on for it to be
// successfully remotely executed.
void FindAllFilesFromCommand(int argc, char** argv, set<string>* files);
}

#endif  // REMOTE_CLIENT_JAVAC_REMOTE_ACTIONS_H_