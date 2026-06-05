#include "tests/cpp/ymq/common/utils.h"

#include <filesystem>

// change the current working directory to the project root
// this is important for finding the python mitm script
void chdirToProjectRoot()
{
    auto cwd = std::filesystem::current_path();

    // if pyproject.toml is in `path`, it's the project root
    for (auto path = cwd; !path.empty(); path = path.parent_path()) {
        if (std::filesystem::exists(path / "pyproject.toml")) {
            // change to the project root
            std::filesystem::current_path(path);
            return;
        }
    }
}
