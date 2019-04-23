#ifndef REMOTE_CLIENT_JAVAC_REMOTE_ACTIONS_H_
#define REMOTE_CLIENT_JAVAC_REMOTE_ACTIONS_H_

namespace remote_client {
// IsJavacAction determines if the given action is a java compile action.
bool IsJavacAction(int argc, char** argv);
}

#endif  // REMOTE_CLIENT_JAVAC_REMOTE_ACTIONS_H_
