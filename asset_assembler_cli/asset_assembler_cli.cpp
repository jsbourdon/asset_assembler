#include "asset_assembler/database/AssetDatabaseBuilder.h"
#include "Salvation_Common/Memory/ThreadHeapAllocator.h"
#include "Salvation_Common/Core/Defines.h"

using namespace asset_assembler::database;
using namespace salvation::memory;

int main()
{
    // All heavy memory allocations must go through salvation::memory::VirtualMemoryAllocator.
    ThreadHeapAllocator::Init(GiB(1), MiB(10));
    AssetDatabaseBuilder::BuildDatabase("C:/Temp/bulbasaur/scene.gltf", "C:/Temp/AutoCreated.db");

    return 0;
}

