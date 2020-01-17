#pragma once

namespace asset_assembler
{
    namespace database
    {
        class AssetDatabaseBuilder
        {
        public:

            static bool BuildDatabase(const char *pSrcPath, const char *pDstPath);

        private:

            AssetDatabaseBuilder() = delete;
            ~AssetDatabaseBuilder() = delete;
        };
    }
}