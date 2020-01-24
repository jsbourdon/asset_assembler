#include "asset_assembler/database/AssetDatabaseBuilder.h"
#include "Salvation_Common/Memory/ThreadHeapAllocator.h"
#include "Salvation_Common/Core/Defines.h"
#include "Salvation_Common/FileSystem/FileSystem.h"

using namespace asset_assembler::database;
using namespace salvation::memory;
using namespace salvation;

int main()
{
    // All heavy memory allocations must go through salvation::memory::VirtualMemoryAllocator.
    ThreadHeapAllocator::Init(GiB(1), MiB(100));
    AssetDatabaseBuilder builder;
    builder.BuildDatabase("D:/Temp/bulbasaur/scene.gltf", "D:/Temp/Assets/AssetsDB.db");

    return 0;
}

