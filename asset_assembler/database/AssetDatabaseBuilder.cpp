#include <pch.h>
#include <cstdio>
#include "AssetDatabaseBuilder.h"
#include "Salvation_Common/Memory/ThreadHeapAllocator.h"
#include "sqlite/sqlite3.h"
#include "rapidjson/document.h"

using namespace asset_assembler::database;
using namespace salvation::memory;
using namespace rapidjson;

struct SQLiteRAII
{
    SQLiteRAII(sqlite3 *pDb) : m_pDb(pDb) {}
    ~SQLiteRAII() { sqlite3_close(m_pDb); }
    sqlite3 *m_pDb;
};

struct StatementRAII
{
    StatementRAII(sqlite3_stmt *pStmt) : m_pStmt(pStmt) {}
    ~StatementRAII() { sqlite3_finalize(m_pStmt); }
    sqlite3_stmt *m_pStmt;
};

static bool CreateTables(sqlite3 *pDb)
{
    static constexpr char pCreateMeshTable[] = R"(
    CREATE TABLE IF NOT EXISTS Mesh
    (
        ID int IDENTITY(1,1) PRIMARY KEY,
        Name varchar(255)
    );)";

    static constexpr char pCreatePackedDataTable[] = R"(
    CREATE TABLE IF NOT EXISTS PackedData
    (
        ID int IDENTITY(1,1) PRIMARY KEY,
        FilePath varchar(255) NOT NULL
    );)";

    static constexpr char pCreateTextureTable[] = R"(
    CREATE TABLE IF NOT EXISTS Texture
    (
        ID int IDENTITY(1,1) PRIMARY KEY,
        ByteSize int NOT NULL,
        ByteOffset int NOT NULL,
        Format int NOT NULL,
        PackedDataID int NOT NULL,
        FOREIGN KEY(PackedDataID) REFERENCES PackedData(ID)
    );)";

    static constexpr char pCreateBufferTable[] = R"(
    CREATE TABLE IF NOT EXISTS Buffer
    (
        ID int IDENTITY(1,1) PRIMARY KEY,
        ByteSize int NOT NULL,
        ByteOffset int NOT NULL,
        PackedDataID int NOT NULL,
        FOREIGN KEY(PackedDataID) REFERENCES PackedData(ID)
    );)";

    static constexpr char pCreateBufferViewTable[] = R"(
    CREATE TABLE IF NOT EXISTS BufferView
    (
        ID int IDENTITY(1,1) PRIMARY KEY,
        ByteSize int NOT NULL,
        ByteOffset int NOT NULL,
        Attribute int NOT NULL,
        BufferID int NOT NULL,
        FOREIGN KEY(BufferID) REFERENCES Buffer(ID)
    );)";

    static constexpr char pCreateMaterialTable[] = R"(
    CREATE TABLE IF NOT EXISTS Material
    (
        ID int IDENTITY(1,1) PRIMARY KEY,
        DiffuseTextureID int NOT NULL,
        FOREIGN KEY(DiffuseTextureID) REFERENCES Texture(ID)
    );)";

    static constexpr char pCreateSubMeshTable[] = R"(
    CREATE TABLE IF NOT EXISTS SubMesh
    (
        ID int IDENTITY(1,1) PRIMARY KEY,
        IndexBufferID int NOT NULL,
        MaterialID int,
        FOREIGN KEY(IndexBufferID) REFERENCES BufferView(ID),
        FOREIGN KEY(MaterialID) REFERENCES Material(ID)
    );)";

    static constexpr char pCreateMeshSubMeshTable[] = R"(
    CREATE TABLE IF NOT EXISTS MeshSubMesh
    (
        MeshID int NOT NULL,
        SubMeshID int NOT NULL,
        PRIMARY KEY(MeshID, SubMeshID),
        FOREIGN KEY(MeshID) REFERENCES Mesh(ID),
        FOREIGN KEY(SubMeshID) REFERENCES SubMesh(ID)
    );)";

    static constexpr char pCreateSubMeshVertexStreamsTable[] = R"(
    CREATE TABLE IF NOT EXISTS SubMeshVertexStreams
    (
        SubMeshID int NOT NULL,
        BufferViewID int NOT NULL,
        PRIMARY KEY(SubMeshID, BufferViewID),
        FOREIGN KEY(SubMeshID) REFERENCES SubMesh(ID),
        FOREIGN KEY(BufferViewID) REFERENCES BufferView(ID)
    );)";

    static constexpr const char* ppCreateTableStmt[] =
    {
        pCreateMeshTable,
        pCreatePackedDataTable,
        pCreateTextureTable,
        pCreateBufferTable,
        pCreateBufferViewTable,
        pCreateMaterialTable,
        pCreateSubMeshTable,
        pCreateMeshSubMeshTable,
        pCreateSubMeshVertexStreamsTable
    };

    static constexpr const int createTableStmtLength[] =
    {
        static_cast<int>(sizeof(pCreateMeshTable)),
        static_cast<int>(sizeof(pCreatePackedDataTable)),
        static_cast<int>(sizeof(pCreateTextureTable)),
        static_cast<int>(sizeof(pCreateBufferTable)),
        static_cast<int>(sizeof(pCreateBufferViewTable)),
        static_cast<int>(sizeof(pCreateMaterialTable)),
        static_cast<int>(sizeof(pCreateSubMeshTable)),
        static_cast<int>(sizeof(pCreateMeshSubMeshTable)),
        static_cast<int>(sizeof(pCreateSubMeshVertexStreamsTable))
    };

    int result = SQLITE_OK;

    for (size_t i = 0; i < ARRAY_SIZE(createTableStmtLength); ++i)
    {
        sqlite3_stmt* createTablesStmt = nullptr;

        result = sqlite3_prepare_v2(pDb, ppCreateTableStmt[i], createTableStmtLength[i], &createTablesStmt, nullptr);
        StatementRAII stmtRAII(createTablesStmt);

        if (result != SQLITE_OK) break;

        result = sqlite3_step(createTablesStmt);
        if (result != SQLITE_DONE) break;
    }

    return result == SQLITE_DONE;
}

static sqlite3* CreateDatabase(const char *pDstPath)
{
    sqlite3 *pDb = nullptr;
    int dbResult = sqlite3_open(pDstPath, &pDb);
    if (dbResult == SQLITE_OK)
    {
        if (CreateTables(pDb))
        {
            return pDb;
        }

        // Creating tables failed. Close and bail out.
        sqlite3_close(pDb);
    }

    return nullptr;
}

static bool BuildTextures(Document &json, const char *pSrcRootPath, const char *pTextureBinPath)
{
    /*
    CMP_MipSet mipSet = {};
    CMP_ERROR result = CMP_LoadTexture("C:\\Temp\\bulbasaur\\textures\\Default_baseColor.png", &mipSet);

    if (result == CMP_OK)
    {

    }
    */

    /*
    static constexpr const char *s_pImgProperty = "images";
    static constexpr const char *s_pUriProperty = "uri";

    if (json.HasMember(s_pImgProperty) && json[s_pImgProperty].IsArray())
    {
        Value &images = json[s_pImgProperty];
        SizeType imageCount = images.Size();
        for (SizeType i = 0; i < imageCount; ++i)
        {
            Value &img = images[i];
            if (img.HasMember(s_pUriProperty) && img[s_pUriProperty].IsString())
            {
                Value &uri = img[s_pUriProperty];
                const char *pTextureUri = uri.GetString();
            }
        }
    }
    */

    return false;
}

bool AssetDatabaseBuilder::BuildDatabase(const char *pSrcPath, const char *pDstPath)
{
    // #todo Add some decent error handling O_o

    sqlite3 *pDb = CreateDatabase(pDstPath);
    if (pDb)
    {
        SQLiteRAII dbRAII(pDb);

        FILE *pJsonFile;
        errno_t err = fopen_s(&pJsonFile, pSrcPath, "r");

        if (err == 0 && pJsonFile)
        {
            fseek(pJsonFile, 0, SEEK_END);
            size_t fileSize = ftell(pJsonFile);
            fseek(pJsonFile, 0, SEEK_SET);

            char *pJsonContent = static_cast<char*>(ThreadHeapAllocator::Allocate(fileSize));
            fread(pJsonContent, sizeof(char), fileSize, pJsonFile);
            fclose(pJsonFile);

            Document json;
            json.Parse(pJsonContent);

            ThreadHeapAllocator::Release(pJsonContent);

            static constexpr char s_pTexturesBinFileName[] = "Textures.bin";

            const char *pDstRootPathEnd = strrchr(pDstPath, '/');
            const char *pSrcRootPathEnd = strrchr(pSrcPath, '/');

            if (pDstRootPathEnd && pSrcRootPathEnd)
            {
                size_t dstRootFolderStrLen = 
                    static_cast<size_t>(reinterpret_cast<uintptr_t>(pDstRootPathEnd) - reinterpret_cast<uintptr_t>(pDstPath)) + 1;
                size_t srcRootFolderStrLen =
                    static_cast<size_t>(reinterpret_cast<uintptr_t>(pSrcRootPathEnd) - reinterpret_cast<uintptr_t>(pSrcPath)) + 1;
                size_t texturesBinFilePathStrLen = sizeof(s_pTexturesBinFileName) + dstRootFolderStrLen;

                char *pTextureDstPath = static_cast<char*>(salvation::memory::StackAlloc(texturesBinFilePathStrLen + 1));
                char *pSrcRootPath = static_cast<char*>(salvation::memory::StackAlloc(srcRootFolderStrLen + 1));

                pTextureDstPath[texturesBinFilePathStrLen] = 0;
                pSrcRootPath[srcRootFolderStrLen] = 0;

                memcpy(pTextureDstPath, pDstPath, dstRootFolderStrLen);
                memcpy(pTextureDstPath + dstRootFolderStrLen, s_pTexturesBinFileName, sizeof(s_pTexturesBinFileName));
                memcpy(pSrcRootPath, pSrcPath, srcRootFolderStrLen);

                BuildTextures(json, pSrcRootPath, pTextureDstPath);
            }

            /*
            fs::path dstPath(pDstPath);
            fs::path textureDstPath = dstPath.replace_filename("Textures").replace_extension(".bin");

            fs::path srcPath(pSrcPath);
            fs::path srcRoot = srcPath.remove_filename();

            if (BuildTextures(json, srcRoot.string().c_str(), textureDstPath.string().c_str()))
            {

            }
            */
        }
    }

    return false;
}
