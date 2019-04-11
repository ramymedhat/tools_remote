#ifndef REMOTE_CLIENT_JAVAC_REMOTE_ACTIONS_H_
#define REMOTE_CLIENT_JAVAC_REMOTE_ACTIONS_H_

#include <set>
#include <string>

namespace remote_client {

using std::set;
using std::string;

// DetermineJavacActionInputs finds the inputs needed to run the javac action
// remotely from the given input command. These inputs are in-addition to the inputs
// passed on by ninja itself.
// For Android-9 builds, this function returns the files that have "@" prefix in the
// given input command.
void DetermineJavacActionInputs(int argc, char** argv, set<string>* inputs);

// IsJavacAction determines if the given action is a java compile action.
bool IsJavacAction(int argc, char** argv);

}

#endif  // REMOTE_CLIENT_JAVAC_REMOTE_ACTIONS_H_