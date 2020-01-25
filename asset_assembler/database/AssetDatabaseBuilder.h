#pragma once

#include <cstdint>
#include <stdio.h>
#include "asset_assembler/rapidjson/fwd.h"

struct sqlite3;
struct sqlite3_stmt;

namespace salvation::asset { enum class PackedDataType; }

using namespace rapidjson;

namespace asset_assembler
{
    namespace database
    {
        class AssetDatabaseBuilder
        {
        public:

            AssetDatabaseBuilder() = default;
            ~AssetDatabaseBuilder() = default;

            bool BuildDatabase(const char *pSrcPath, const char *pDstPath);

        private:

            static constexpr size_t s_MaxRscFilePathLen = 1024;

            struct StatementRAII
            {
                StatementRAII(sqlite3_stmt *pStmt) : m_pStmt(pStmt) {}
                ~StatementRAII();
                sqlite3_stmt *m_pStmt;
            };

            struct InsertStatements
            {
                sqlite3_stmt*   m_pPackedDataStmt;
                sqlite3_stmt*   m_pTextureStmt;
                sqlite3_stmt*   m_pBufferStmt;
                sqlite3_stmt*   m_pMaterialStmt;
                sqlite3_stmt*   m_pBufferViewStmt;
            };

            static uint8_t* ReadFileContent(const char *pSrcPath, size_t &o_FileSize);

            void        ReleaseResources();

            bool        CreateDatabase(const char *pDstPath);
            bool        CreateTables();

            bool        CreateInsertStatements();
            void        ReleaseInsertStatements();
            
            int64_t     InsertPackagedDataEntry(const char *pFilePath, salvation::asset::PackedDataType dataType);
            bool        InsertTextureDataEntry(int64_t byteSize, int64_t byteOffset, int32_t format, int64_t packedDataId);
            bool        InsertBufferDataEntry(int64_t byteSize, int64_t byteOffset, int64_t packedDataId);
            bool        InsertMaterialDataEntry(int64_t textureId);
            bool        InsertBufferViewDataEntry(int64_t bufferId, int64_t byteSize, int64_t byteOffset, int32_t componentType);

            bool        InsertMaterialMetadata(Document &json);
            bool        InsertBufferViewMetadata(Document &json);
            bool        InsertMetadata(Document &json);

            bool        BuildTextures(Document &json, const char *pSrcRootPath, const char *pDestRootPath);
            int64_t     CompressTexture(const char *pSrcFilePath, FILE *pDestFile, int64_t packedDataId);
            bool        BuildMeshes(Document &json, const char *pSrcRootPath, const char *pDestRootPath);

        private:

            sqlite3*            m_pDb { nullptr };
            InsertStatements    m_Stmts {};
        };
    }
}