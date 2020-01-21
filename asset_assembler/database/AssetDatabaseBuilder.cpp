#include <pch.h>
#include <cstdio>
#include "AssetDatabaseBuilder.h"
#include "Salvation_Common/Memory/ThreadHeapAllocator.h"
#include "Salvation_Common/Memory/ThreadHeapSmartPointer.h"
#include "sqlite/sqlite3.h"
#include "rapidjson/document.h"
#include "3rd/Compressonator/Compressonator/CMP_Framework/CMP_Framework.h"

using namespace asset_assembler::database;
using namespace salvation::memory;
using namespace rapidjson;

using str_smart_ptr = ThreadHeapSmartPointer<char>;

static constexpr size_t s_MaxTextureFilePathLen = 1024;

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


static uint8_t* ReadFileContent(const char *pSrcPath, size_t &o_FileSize)
{
    uint8_t *pContent = nullptr;
    FILE *pFile;
    errno_t err = fopen_s(&pFile, pSrcPath, "r");
    o_FileSize = 0;

    if (err == 0 && pFile)
    {
        fseek(pFile, 0, SEEK_END);
        size_t fileSize = ftell(pFile);
        fseek(pFile, 0, SEEK_SET);

        pContent = static_cast<uint8_t*>(ThreadHeapAllocator::Allocate(fileSize));

        fread(pContent, sizeof(uint8_t), fileSize, pFile);
        fclose(pFile);

        o_FileSize = fileSize;
    }

    return pContent;
}

/// CMP_Feedback_Proc
/// Feedback function for conversion.
/// \param[in] fProgress The percentage progress of the texture compression.
/// \return non-NULL(true) value to abort conversion
bool CMP_Feedback(CMP_FLOAT fProgress, CMP_DWORD_PTR pUser1, CMP_DWORD_PTR pUser2)
{
    return false;
}

static bool CompressTexture(const char *pSrcFilePath, const char *pDestFilePath)
{
    CMP_MipSet mipSetIn = {};
    CMP_ERROR result = CMP_LoadTexture(pSrcFilePath, &mipSetIn);

    if (result == CMP_OK)
    {
        // Generate MIP chain if not already generated
        if (mipSetIn.m_nMipLevels <= 1)
        {
            static constexpr CMP_INT s_MinMipSize = 4; // 4x4
            CMP_GenerateMIPLevels(&mipSetIn, s_MinMipSize);
        }

        // Compress texture into BC3 for now #todo provide format as argument
        {
            KernelOptions kernelOptions = {};
            kernelOptions.format = CMP_FORMAT_BC3;
            kernelOptions.fquality = 1.0f;
            kernelOptions.threads = 0; // Auto setting

            CMP_MipSet mipSetOut = {};
            result = CMP_ProcessTexture(&mipSetIn, &mipSetOut, kernelOptions, &CMP_Feedback);

            if (result == CMP_OK)
            {
                result = CMP_SaveTexture(pDestFilePath, &mipSetOut);

                return (result == CMP_OK);
            }
        }
    }

    return (result == CMP_OK);
}

static bool PackageCompressedTextures(const char *pCompressedFilePaths, size_t fileCount, const char *pDestRootPath)
{
    static constexpr char s_pTexturesBinFileName[] = "Textures.bin";
    static constexpr size_t s_TexturesBinFileNameLen = sizeof(s_pTexturesBinFileName) - 1;

    size_t destRootPathLen = strlen(pDestRootPath);
    size_t destFilePathLen = destRootPathLen + s_TexturesBinFileNameLen + 1;
    char *pDestFilePath = static_cast<char*>(salvation::memory::StackAlloc(destFilePathLen));
    pDestFilePath[destFilePathLen] = 0;

    memcpy(pDestFilePath, pDestRootPath, destRootPathLen);
    memcpy(pDestFilePath + destRootPathLen, s_pTexturesBinFileName, s_TexturesBinFileNameLen);

    FILE *pFile;
    errno_t err = fopen_s(&pFile, pDestFilePath, "wb");

    if (err == 0 && pFile)
    {
        for (size_t i = 0; i < fileCount; ++i)
        {
            size_t fileSize;
            const char *pCompressedFilePath = pCompressedFilePaths + (i * s_MaxTextureFilePathLen);
            const uint8_t *pCompressedFile = ReadFileContent(pCompressedFilePath, fileSize);

            fwrite(pCompressedFile, sizeof(uint8_t), fileSize, pFile);
        }

        fclose(pFile);
    }

    return true;
}

static bool BuildTextures(Document &json, const char *pSrcRootPath, const char *pDestRootPath)
{
    static constexpr const char *s_pImgProperty = "images";
    static constexpr const char *s_pUriProperty = "uri";
    static constexpr const char s_DdsExt[] = ".dds";

    size_t srcRootPathLen = strlen(pSrcRootPath);
    size_t destRootPathLen = strlen(pDestRootPath);

    if (json.HasMember(s_pImgProperty) && json[s_pImgProperty].IsArray())
    {
        Value &images = json[s_pImgProperty];
        SizeType imageCount = images.Size();

        str_smart_ptr compressedPaths = ThreadHeapAllocator::Allocate(s_MaxTextureFilePathLen * imageCount);

        for (SizeType i = 0; i < imageCount; ++i)
        {
            Value &img = images[i];
            if (img.HasMember(s_pUriProperty) && img[s_pUriProperty].IsString())
            {
                Value &uri = img[s_pUriProperty];
                const char *pTextureUri = uri.GetString();

                const char *extPoint = strrchr(pTextureUri, '.');
                if (extPoint)
                {
                    // Source texture file path
                    size_t uriLen = strlen(pTextureUri);
                    size_t srcFilePathLen = srcRootPathLen + uriLen;
                    char *pSrcFilePath = reinterpret_cast<char*>(ThreadHeapAllocator::Allocate(srcFilePathLen + 1));
                    pSrcFilePath[srcFilePathLen] = 0;
                    memcpy(pSrcFilePath, pSrcRootPath, srcRootPathLen);
                    memcpy(pSrcFilePath + srcRootPathLen, pTextureUri, uriLen);

                    // Destination compressed texture file path
                    size_t filePathNoExtLen =
                        static_cast<size_t>(reinterpret_cast<uintptr_t>(extPoint) - reinterpret_cast<uintptr_t>(pTextureUri));
                    size_t dstLen = destRootPathLen + filePathNoExtLen + sizeof(s_DdsExt);
                    char *pCompressedPath = compressedPaths + (s_MaxTextureFilePathLen * i);
                    pCompressedPath[dstLen] = 0;
                    memcpy(pCompressedPath, pDestRootPath, destRootPathLen);
                    memcpy(pCompressedPath + destRootPathLen, pTextureUri, filePathNoExtLen);
                    memcpy(pCompressedPath + destRootPathLen + filePathNoExtLen, s_DdsExt, sizeof(s_DdsExt));

                    CompressTexture(pSrcFilePath, pCompressedPath);
                }
            }
        }

        PackageCompressedTextures(compressedPaths, imageCount, pDestRootPath);
    }

    return false;
}

bool AssetDatabaseBuilder::BuildDatabase(const char *pSrcPath, const char *pDstPath)
{
    // #todo Add some decent error handling O_o

    sqlite3 *pDb = CreateDatabase(pDstPath);
    if (pDb)
    {
        SQLiteRAII dbRAII(pDb);

        size_t jsonContentSize;
        char *pJsonContent = reinterpret_cast<char*>(ReadFileContent(pSrcPath, jsonContentSize));

        if (pJsonContent)
        {
            Document json;
            json.Parse(pJsonContent);

            ThreadHeapAllocator::Release(pJsonContent);

            const char *pDstRootPathEnd = strrchr(pDstPath, '/');
            const char *pSrcRootPathEnd = strrchr(pSrcPath, '/');

            if (pDstRootPathEnd && pSrcRootPathEnd)
            {
                size_t dstRootFolderStrLen = 
                    static_cast<size_t>(reinterpret_cast<uintptr_t>(pDstRootPathEnd) - reinterpret_cast<uintptr_t>(pDstPath)) + 1;
                size_t srcRootFolderStrLen =
                    static_cast<size_t>(reinterpret_cast<uintptr_t>(pSrcRootPathEnd) - reinterpret_cast<uintptr_t>(pSrcPath)) + 1;

                char *pTextureDstPath = static_cast<char*>(salvation::memory::StackAlloc(dstRootFolderStrLen + 1));
                char *pSrcRootPath = static_cast<char*>(salvation::memory::StackAlloc(srcRootFolderStrLen + 1));

                pTextureDstPath[dstRootFolderStrLen] = 0;
                pSrcRootPath[srcRootFolderStrLen] = 0;

                memcpy(pTextureDstPath, pDstPath, dstRootFolderStrLen);
                memcpy(pSrcRootPath, pSrcPath, srcRootFolderStrLen);

                BuildTextures(json, pSrcRootPath, pTextureDstPath);
            }
        }
    }

    return false;
}
