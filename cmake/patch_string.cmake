# patch_string.cmake — Replace a string in a file (used for gRPC service path fix)
# Usage: cmake -DFILE=<path> -DOLD_STR=<old> -DNEW_STR=<new> -P patch_string.cmake
file(READ "${FILE}" content)
string(REPLACE "${OLD_STR}" "${NEW_STR}" content "${content}")
file(WRITE "${FILE}" "${content}")
