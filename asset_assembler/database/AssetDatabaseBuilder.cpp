#include <pch.h>
#include "AssetDatabaseBuilder.h"
#include "Salvation_Common/Memory/ThreadHeapAllocator.h"
#include "Salvation_Common/Memory/ThreadHeapSmartPointer.h"
#include "Salvation_Common/FileSystem/FileSystem.h"
#include "Salvation_Common/Assets/AssetDatabase.h"
#include "sqlite/sqlite3.h"
#include "rapidjson/document.h"
#include "3rd/Compressonator/Compressonator/CMP_Framework/CMP_Framework.h"

using namespace asset_assembler::database;
using namespace salvation;
using namespace salvation::asset;
using namespace salvation::memory;


AssetDatabaseBuilder::StatementRAII::~StatementRAII() 
{ 
    sqlite3_finalize(m_pStmt); 
}

void AssetDatabaseBuilder::ReleaseResources()
{
    ReleaseInsertStatements();

    if (m_pDb)
    {
        sqlite3_close(m_pDb);
    }
}

bool AssetDatabaseBuilder::CreateInsertStatements()
{
    static constexpr char s_PackedDataStr[] = "INSERT INTO PackedData(FilePath, DataType) VALUES (?1, ?2);";
    static constexpr char s_TextureStr[] = "INSERT INTO Texture(ByteSize, ByteOffset, Format, PackedDataID) VALUES(?1, ?2, ?3, ?4);";
    static constexpr char s_BufferStr[] = "INSERT INTO Buffer(ByteSize, ByteOffset, PackedDataID) VALUES(?1, ?2, ?3);";

    return
        sqlite3_prepare_v2(m_pDb, s_PackedDataStr, -1, &m_Stmts.m_pPackedDataStmt, nullptr) == SQLITE_OK &&
        sqlite3_prepare_v2(m_pDb, s_TextureStr, -1, &m_Stmts.m_pTextureStmt, nullptr) == SQLITE_OK &&
        sqlite3_prepare_v2(m_pDb, s_BufferStr, -1, &m_Stmts.m_pBufferStmt, nullptr) == SQLITE_OK;
}

void AssetDatabaseBuilder::ReleaseInsertStatements()
{
    if (m_Stmts.m_pPackedDataStmt) sqlite3_finalize(m_Stmts.m_pPackedDataStmt);
    if (m_Stmts.m_pTextureStmt) sqlite3_finalize(m_Stmts.m_pTextureStmt);
    if (m_Stmts.m_pBufferStmt) sqlite3_finalize(m_Stmts.m_pBufferStmt);
}

bool AssetDatabaseBuilder::CreateTables()
{
    static constexpr char pCreateMeshTable[] = R"(
    CREATE TABLE IF NOT EXISTS Mesh
    (
        ID INTEGER PRIMARY KEY,
        Name varchar(255)
    );)";

    static constexpr char pCreatePackedDataTable[] = R"(
    CREATE TABLE IF NOT EXISTS PackedData
    (
        ID INTEGER PRIMARY KEY,
        FilePath varchar(255) NOT NULL,
        DataType INTEGER NOT NULL
    );)";

    static constexpr char pCreateTextureTable[] = R"(
    CREATE TABLE IF NOT EXISTS Texture
    (
        ID INTEGER PRIMARY KEY,
        ByteSize INTEGER NOT NULL,
        ByteOffset INTEGER NOT NULL,
        Format INTEGER NOT NULL,
        PackedDataID INTEGER NOT NULL,
        FOREIGN KEY(PackedDataID) REFERENCES PackedData(ID)
    );)";

    static constexpr char pCreateBufferTable[] = R"(
    CREATE TABLE IF NOT EXISTS Buffer
    (
        ID INTEGER PRIMARY KEY,
        ByteSize INTEGER NOT NULL,
        ByteOffset INTEGER NOT NULL,
        PackedDataID INTEGER NOT NULL,
        FOREIGN KEY(PackedDataID) REFERENCES PackedData(ID)
    );)";

    static constexpr char pCreateBufferViewTable[] = R"(
    CREATE TABLE IF NOT EXISTS BufferView
    (
        ID INTEGER PRIMARY KEY,
        ByteSize INTEGER NOT NULL,
        ByteOffset INTEGER NOT NULL,
        Attribute INTEGER NOT NULL,
        BufferID INTEGER NOT NULL,
        FOREIGN KEY(BufferID) REFERENCES Buffer(ID)
    );)";

    static constexpr char pCreateMaterialTable[] = R"(
    CREATE TABLE IF NOT EXISTS Material
    (
        ID INTEGER PRIMARY KEY,
        DiffuseTextureID INTEGER NOT NULL,
        FOREIGN KEY(DiffuseTextureID) REFERENCES Texture(ID)
    );)";

    static constexpr char pCreateSubMeshTable[] = R"(
    CREATE TABLE IF NOT EXISTS SubMesh
    (
        ID INTEGER PRIMARY KEY,
        IndexBufferID INTEGER NOT NULL,
        MaterialID INTEGER,
        FOREIGN KEY(IndexBufferID) REFERENCES BufferView(ID),
        FOREIGN KEY(MaterialID) REFERENCES Material(ID)
    );)";

    static constexpr char pCreateMeshSubMeshTable[] = R"(
    CREATE TABLE IF NOT EXISTS MeshSubMesh
    (
        MeshID INTEGER NOT NULL,
        SubMeshID INTEGER NOT NULL,
        PRIMARY KEY(MeshID, SubMeshID),
        FOREIGN KEY(MeshID) REFERENCES Mesh(ID),
        FOREIGN KEY(SubMeshID) REFERENCES SubMesh(ID)
    );)";

    static constexpr char pCreateSubMeshVertexStreamsTable[] = R"(
    CREATE TABLE IF NOT EXISTS SubMeshVertexStreams
    (
        SubMeshID INTEGER NOT NULL,
        BufferViewID INTEGER NOT NULL,
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

    int result = SQLITE_OK;

    for (size_t i = 0; i < ARRAY_SIZE(ppCreateTableStmt); ++i)
    {
        sqlite3_stmt* createTablesStmt = nullptr;

        result = sqlite3_prepare_v2(m_pDb, ppCreateTableStmt[i], -1, &createTablesStmt, nullptr);
        StatementRAII stmtRAII(createTablesStmt);

        if (result != SQLITE_OK) break;

        result = sqlite3_step(createTablesStmt);
        if (result != SQLITE_DONE) break;
    }

    return result == SQLITE_DONE;
}

bool AssetDatabaseBuilder::CreateDatabase(const char *pDstPath)
{
    str_smart_ptr dirPath = filesystem::ExtractDirectoryPath(pDstPath);
    if (!filesystem::DirectoryExists(dirPath))
    {
        if (!filesystem::CreateDirectory(dirPath))
        {
            return false;
        }
    }

    int dbResult = sqlite3_open(pDstPath, &m_pDb);
    if (dbResult == SQLITE_OK)
    {
        return CreateTables();
    }

    return false;
}

uint8_t* AssetDatabaseBuilder::ReadFileContent(const char *pSrcPath, size_t &o_FileSize)
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
static bool CMP_Feedback(CMP_FLOAT fProgress, CMP_DWORD_PTR pUser1, CMP_DWORD_PTR pUser2)
{
    return false;
}

bool AssetDatabaseBuilder::CompressTexture(const char *pSrcFilePath, const char *pDestFilePath)
{
    CMP_MipSet mipSetIn = {};
    CMP_ERROR result = CMP_LoadTexture(pSrcFilePath, &mipSetIn);

    if (result == CMP_OK)
    {
        // Create destination folder if necessary
        {
            str_smart_ptr dirPath = filesystem::ExtractDirectoryPath(pDestFilePath);
            if (!filesystem::DirectoryExists(dirPath))
            {
                if (!filesystem::CreateDirectory(dirPath))
                {
                    return false;
                }
            }
        }

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
            }
        }
    }

    return (result == CMP_OK);
}

int64_t AssetDatabaseBuilder::InsertPackagedDataEntry(const char *pFilePath, salvation::asset::PackedDataType dataType)
{
    int64_t packageID = -1;
    sqlite3_stmt *pStmt = m_Stmts.m_pPackedDataStmt;

    if (
        sqlite3_reset(pStmt) == SQLITE_OK && 
        sqlite3_bind_text(pStmt, 1, pFilePath, -1, SQLITE_STATIC) == SQLITE_OK &&
        sqlite3_bind_int(pStmt, 2, static_cast<int>(dataType)) == SQLITE_OK &&
        sqlite3_step(pStmt) == SQLITE_DONE)
    {
        packageID = sqlite3_last_insert_rowid(m_pDb);
    }

    return packageID;
}

bool AssetDatabaseBuilder::InsertTextureDataEntry(int64_t byteSize, int64_t byteOffset, int32_t format, int64_t packedDataId)
{
    sqlite3_stmt *pStmt = m_Stmts.m_pTextureStmt;

    return 
        sqlite3_reset(pStmt) == SQLITE_OK &&
        sqlite3_bind_int64(pStmt, 1, byteSize) == SQLITE_OK &&
        sqlite3_bind_int64(pStmt, 2, byteOffset) == SQLITE_OK &&
        sqlite3_bind_int(pStmt, 3, format) == SQLITE_OK &&
        sqlite3_bind_int64(pStmt, 4, packedDataId) == SQLITE_OK &&
        sqlite3_step(pStmt) == SQLITE_DONE;

}

bool AssetDatabaseBuilder::InsertBufferDataEntry(int64_t byteSize, int64_t byteOffset, int64_t packedDataId)
{
    sqlite3_stmt *pStmt = m_Stmts.m_pBufferStmt;

    return
        sqlite3_reset(pStmt) == SQLITE_OK &&
        sqlite3_bind_int64(pStmt, 1, byteSize) == SQLITE_OK &&
        sqlite3_bind_int64(pStmt, 2, byteOffset) == SQLITE_OK &&
        sqlite3_bind_int64(pStmt, 3, packedDataId) == SQLITE_OK &&
        sqlite3_step(pStmt) == SQLITE_DONE;
}

bool AssetDatabaseBuilder::PackageTextures(const char *pCompressedFilePaths, size_t fileCount, const char *pDestRootPath)
{
    static constexpr char s_pTexturesBinFileName[] = "Textures.bin";
    static constexpr size_t s_TexturesBinFileNameLen = sizeof(s_pTexturesBinFileName) - 1;
    static constexpr int32_t s_TextureFormat = static_cast<int32_t>(TextureFormat::BC3);

    bool success = false;

    size_t destRootPathLen = strlen(pDestRootPath);
    size_t destFilePathLen = destRootPathLen + s_TexturesBinFileNameLen;
    char *pDestFilePath = static_cast<char *>(salvation::memory::StackAlloc(destFilePathLen + 1));
    pDestFilePath[destFilePathLen] = 0;

    memcpy(pDestFilePath, pDestRootPath, destRootPathLen);
    memcpy(pDestFilePath + destRootPathLen, s_pTexturesBinFileName, s_TexturesBinFileNameLen);

    FILE *pFile;
    errno_t err = fopen_s(&pFile, pDestFilePath, "wb");

    if (err == 0 && pFile)
    {
        int64_t packedDataId = InsertPackagedDataEntry(s_pTexturesBinFileName, PackedDataType::Textures);

        if (packedDataId >= 0)
        {
            bool writeSucceeded = true;
            int64_t currentByteOffset = 0;

            for (size_t i = 0; i < fileCount && writeSucceeded; ++i)
            {
                size_t fileSize;
                const char *pCompressedFilePath = pCompressedFilePaths + (i * s_MaxRscFilePathLen);
                const uint8_t *pCompressedFile = ReadFileContent(pCompressedFilePath, fileSize);

                writeSucceeded = 
                    fileSize > 0 && 
                    fwrite(pCompressedFile, sizeof(uint8_t), fileSize, pFile) == fileSize &&
                    InsertTextureDataEntry(static_cast<int64_t>(fileSize), currentByteOffset, s_TextureFormat, packedDataId);

                currentByteOffset += static_cast<int64_t>(fileSize);
            }

            fclose(pFile);

            success = writeSucceeded;
        }
    }

    return success;
}

bool AssetDatabaseBuilder::BuildTextures(Document &json, const char *pSrcRootPath, const char *pDestRootPath)
{
    static constexpr const char s_pImgProperty[] = "images";
    static constexpr const char s_pUriProperty[] = "uri";
    static constexpr const char s_DdsExt[] = ".dds";
    static constexpr const char s_TmpDirectoryName[] = "Temp/";
    static constexpr size_t s_TmpDirectoryNameLen = sizeof(s_TmpDirectoryName) - 1;

    size_t srcRootPathLen = strlen(pSrcRootPath);
    size_t destRootPathLen = strlen(pDestRootPath);

    if (json.HasMember(s_pImgProperty) && json[s_pImgProperty].IsArray())
    {
        Value &images = json[s_pImgProperty];
        SizeType imageCount = images.Size();

        if (imageCount > 0)
        {
            str_smart_ptr compressedPaths = ThreadHeapAllocator::Allocate(s_MaxRscFilePathLen * imageCount);

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
                        str_smart_ptr pSrcFilePath = ThreadHeapAllocator::Allocate(srcFilePathLen + 1);
                        pSrcFilePath[srcFilePathLen] = 0;
                        memcpy(pSrcFilePath, pSrcRootPath, srcRootPathLen);
                        memcpy(pSrcFilePath + srcRootPathLen, pTextureUri, uriLen);

                        // Destination compressed texture file path
                        size_t filePathNoExtLen =
                            static_cast<size_t>(reinterpret_cast<uintptr_t>(extPoint) - reinterpret_cast<uintptr_t>(pTextureUri));
                        size_t dstLen = destRootPathLen + filePathNoExtLen + sizeof(s_DdsExt);
                        char *pCompressedPath = compressedPaths + (s_MaxRscFilePathLen * i);
                        pCompressedPath[dstLen] = 0;
                        memcpy(pCompressedPath, pDestRootPath, destRootPathLen);
                        memcpy(pCompressedPath + destRootPathLen, s_TmpDirectoryName, s_TmpDirectoryNameLen);
                        memcpy(pCompressedPath + destRootPathLen + s_TmpDirectoryNameLen, pTextureUri, filePathNoExtLen);
                        memcpy(pCompressedPath + destRootPathLen + s_TmpDirectoryNameLen + filePathNoExtLen, s_DdsExt, sizeof(s_DdsExt));

                        if (!CompressTexture(pSrcFilePath, pCompressedPath))
                        {
                            return false;
                        }
                    }
                }
            }

            return PackageTextures(compressedPaths, imageCount, pDestRootPath);
        }
    }

    return true;
}

bool AssetDatabaseBuilder::BuildMeshes(Document &json, const char *pSrcRootPath, const char *pDestRootPath)
{
    static constexpr const char s_pBuffersBinFileName[] = "Buffers.bin";
    static constexpr const char s_pBuffersProperty[] = "buffers";
    static constexpr const char s_pUriProperty[] = "uri";

    if (json.HasMember(s_pBuffersProperty) && json[s_pBuffersProperty].IsArray())
    {
        Value &buffers = json[s_pBuffersProperty];
        SizeType bufferCount = buffers.Size();

        if (bufferCount > 0)
        {
            int64_t packedDataId = InsertPackagedDataEntry(s_pBuffersBinFileName, PackedDataType::Meshes);
            if (packedDataId < 0)
            {
                return false;
            }

            str_smart_ptr bufferPaths = ThreadHeapAllocator::Allocate(s_MaxRscFilePathLen * bufferCount);

            size_t srcRootPathLen = strlen(pSrcRootPath);
            size_t destRootPathLen = strlen(pDestRootPath);
            size_t destFilePathLen = destRootPathLen + sizeof(s_pBuffersBinFileName);
            str_smart_ptr destFilePath = ThreadHeapAllocator::Allocate(destFilePathLen);
            destFilePath[destFilePathLen - 1] = 0;
            memcpy(destFilePath, pDestRootPath, destRootPathLen);
            memcpy(destFilePath + destRootPathLen, s_pBuffersBinFileName, sizeof(s_pBuffersBinFileName));

            FILE *pDestFile = nullptr;
            if (fopen_s(&pDestFile, destFilePath, "wb") != 0)
            {
                return false;
            }

            bool writeSucceeded = true;
            int64_t currentByteOffset = 0;

            for (SizeType i = 0; i < bufferCount && writeSucceeded; ++i)
            {
                Value &buffer = buffers[i];
                if (buffer.HasMember(s_pUriProperty) && buffer[s_pUriProperty].IsString())
                {
                    Value &uri = buffer[s_pUriProperty];
                    const char *pBufferUri = uri.GetString();

                    // Source texture file path
                    size_t uriLen = strlen(pBufferUri);
                    size_t srcFilePathLen = srcRootPathLen + uriLen;
                    str_smart_ptr pSrcFilePath = ThreadHeapAllocator::Allocate(srcFilePathLen + 1);
                    pSrcFilePath[srcFilePathLen] = 0;
                    memcpy(pSrcFilePath, pSrcRootPath, srcRootPathLen);
                    memcpy(pSrcFilePath + srcRootPathLen, pBufferUri, uriLen);

                    size_t fileSize = 0;
                    uint8_t *pData = ReadFileContent(pSrcFilePath, fileSize);

                    writeSucceeded =
                        fileSize > 0 &&
                        fwrite(pData, sizeof(uint8_t), fileSize, pDestFile) == fileSize &&
                        InsertBufferDataEntry(static_cast<int64_t>(fileSize), currentByteOffset, packedDataId);

                    currentByteOffset += static_cast<int64_t>(fileSize);
                }
            }

            fclose(pDestFile);

            return writeSucceeded;
        }
    }

    return true;
}


bool AssetDatabaseBuilder::BuildDatabase(const char *pSrcPath, const char *pDstPath)
{
    bool success = false;

    if (CreateDatabase(pDstPath))
    {
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

                char *pDstRootPath = static_cast<char*>(salvation::memory::StackAlloc(dstRootFolderStrLen + 1));
                char *pSrcRootPath = static_cast<char*>(salvation::memory::StackAlloc(srcRootFolderStrLen + 1));

                pDstRootPath[dstRootFolderStrLen] = 0;
                pSrcRootPath[srcRootFolderStrLen] = 0;

                memcpy(pDstRootPath, pDstPath, dstRootFolderStrLen);
                memcpy(pSrcRootPath, pSrcPath, srcRootFolderStrLen);

                success = 
                    CreateInsertStatements() && 
                    BuildTextures(json, pSrcRootPath, pDstRootPath) &&
                    BuildMeshes(json, pSrcRootPath, pDstRootPath);
            }
        }
    }

    ReleaseResources();

    return success;
}
