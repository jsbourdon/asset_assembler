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
    static constexpr char s_MaterialStr[] = "INSERT INTO Material(DiffuseTextureID) VALUES(?1);";
    static constexpr char s_BufferViewStr[] = "INSERT INTO BufferView(BufferID, ByteSize, ByteOffset, ComponentType) VALUES((?1, ?2, ?3, ?4);";

    return
        sqlite3_prepare_v2(m_pDb, s_PackedDataStr, -1, &m_Stmts.m_pPackedDataStmt, nullptr) == SQLITE_OK &&
        sqlite3_prepare_v2(m_pDb, s_TextureStr, -1, &m_Stmts.m_pTextureStmt, nullptr) == SQLITE_OK &&
        sqlite3_prepare_v2(m_pDb, s_BufferStr, -1, &m_Stmts.m_pBufferStmt, nullptr) == SQLITE_OK &&
        sqlite3_prepare_v2(m_pDb, s_MaterialStr, -1, &m_Stmts.m_pMaterialStmt, nullptr) == SQLITE_OK &&
        sqlite3_prepare_v2(m_pDb, s_BufferViewStr, -1, &m_Stmts.m_pBufferViewStmt, nullptr) == SQLITE_OK;
}

void AssetDatabaseBuilder::ReleaseInsertStatements()
{
    if (m_Stmts.m_pPackedDataStmt) sqlite3_finalize(m_Stmts.m_pPackedDataStmt);
    if (m_Stmts.m_pTextureStmt) sqlite3_finalize(m_Stmts.m_pTextureStmt);
    if (m_Stmts.m_pBufferStmt) sqlite3_finalize(m_Stmts.m_pBufferStmt);
    if (m_Stmts.m_pMaterialStmt) sqlite3_finalize(m_Stmts.m_pMaterialStmt);
    if (m_Stmts.m_pBufferViewStmt) sqlite3_finalize(m_Stmts.m_pBufferViewStmt);
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
        ComponentType INTEGER NOT NULL,
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
        MeshID INTEGER NOT NULL,
        IndexBufferID INTEGER NOT NULL,
        MaterialID INTEGER,
        FOREIGN KEY(MeshID) REFERENCES Mesh(ID),
        FOREIGN KEY(IndexBufferID) REFERENCES BufferView(ID),
        FOREIGN KEY(MaterialID) REFERENCES Material(ID)
    );)";

    static constexpr char pCreateSubMeshVertexStreamsTable[] = R"(
    CREATE TABLE IF NOT EXISTS SubMeshVertexStreams
    (
        SubMeshID INTEGER NOT NULL,
        BufferViewID INTEGER NOT NULL,
        Attribute INTEGER NOT NULL,
        PRIMARY KEY(SubMeshID, BufferViewID, Attribute),
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

int64_t AssetDatabaseBuilder::CompressTexture(const char *pSrcFilePath, FILE *pDestFile, int64_t packedDataId)
{
    int64_t byteSize = -1;

    CMP_MipSet mipSetIn = {};
    CMP_MipSet mipSetOut = {};

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
            
            result = CMP_ProcessTexture(&mipSetIn, &mipSetOut, kernelOptions, &CMP_Feedback);

            if (result == CMP_OK)
            {
                // #todo Properly save the whole mip chain
                for (int i = 0; i < 1/*mipSetOut.m_nMipLevels*/; ++i)
                {
                    CMP_MipLevel *pMipData;
                    CMP_GetMipLevel(&pMipData, &mipSetOut, i, 0);
                    int64_t mipByteSize = pMipData->m_dwLinearSize;

                    if (fwrite(pMipData->m_pbData, sizeof(uint8_t), mipByteSize, pDestFile) != mipByteSize)
                    {
                        byteSize = -1;
                        break;
                    }

                    byteSize += mipByteSize;
                }
            }
        }
    }

    CMP_FreeMipSet(&mipSetIn);
    CMP_FreeMipSet(&mipSetOut);

    return byteSize;
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

bool AssetDatabaseBuilder::InsertMaterialDataEntry(int64_t textureId)
{
    sqlite3_stmt *pStmt = m_Stmts.m_pMaterialStmt;

    return
        sqlite3_reset(pStmt) == SQLITE_OK &&
        sqlite3_bind_int64(pStmt, 1, textureId) == SQLITE_OK &&
        sqlite3_step(pStmt) == SQLITE_DONE;
}

bool AssetDatabaseBuilder::InsertBufferViewDataEntry(int64_t bufferId, int64_t byteSize, int64_t byteOffset, int32_t componentType)
{
    sqlite3_stmt *pStmt = m_Stmts.m_pBufferViewStmt;

    return
        sqlite3_reset(pStmt) == SQLITE_OK &&
        sqlite3_bind_int64(pStmt, 1, bufferId) == SQLITE_OK &&
        sqlite3_bind_int64(pStmt, 2, byteSize) == SQLITE_OK &&
        sqlite3_bind_int64(pStmt, 3, byteOffset) == SQLITE_OK &&
        sqlite3_bind_int(pStmt, 4, componentType) == SQLITE_OK &&
        sqlite3_step(pStmt) == SQLITE_DONE;
}

bool AssetDatabaseBuilder::BuildTextures(Document &json, const char *pSrcRootPath, const char *pDestRootPath)
{
    static constexpr const char s_pTexturesBinFileName[] = "Textures.bin";
    static constexpr const char s_pImgProperty[] = "images";
    static constexpr const char s_pUriProperty[] = "uri";

    if (json.HasMember(s_pImgProperty) && json[s_pImgProperty].IsArray())
    {
        Value &images = json[s_pImgProperty];
        SizeType imageCount = images.Size();

        if (imageCount > 0)
        {
            str_smart_ptr pDestFilePath = salvation::filesystem::AppendPaths(pDestRootPath, s_pTexturesBinFileName);
            FILE *pDestFile = nullptr;

            if (fopen_s(&pDestFile, pDestFilePath, "wb") != 0)
            {
                return false;
            }

            int64_t packedDataId = InsertPackagedDataEntry(s_pTexturesBinFileName, PackedDataType::Textures);
            if (packedDataId < 0)
            {
                return false;
            }

            int64_t currentByteOffset = 0;

            for (SizeType i = 0; i < imageCount; ++i)
            {
                Value &img = images[i];
                if (img.HasMember(s_pUriProperty) && img[s_pUriProperty].IsString())
                {
                    Value &uri = img[s_pUriProperty];
                    const char *pTextureUri = uri.GetString();

                    str_smart_ptr pSrcFilePath = salvation::filesystem::AppendPaths(pSrcRootPath, pTextureUri);
                    int64_t textureByteSize = CompressTexture(pSrcFilePath, pDestFile, packedDataId);
                    if (textureByteSize <= 0)
                    {
                        return false;
                    }

                    if (!InsertTextureDataEntry(textureByteSize, currentByteOffset, static_cast<int32_t>(TextureFormat::BC3), packedDataId))
                    {
                        return false;
                    }

                    currentByteOffset += textureByteSize;
                }
            }
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
            str_smart_ptr destFilePath = salvation::filesystem::AppendPaths(pDestRootPath, s_pBuffersBinFileName);

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

                    size_t fileSize = 0;
                    str_smart_ptr pSrcFilePath = salvation::filesystem::AppendPaths(pSrcRootPath, pBufferUri);
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

bool AssetDatabaseBuilder::InsertMaterialMetadata(Document &json)
{
    static constexpr const char s_pMaterialsProperty[] = "materials";
    static constexpr const char s_pPBRProperty[] = "pbrMetallicRoughness";
    static constexpr const char s_pBaseTextureProperty[] = "baseColorTexture";
    static constexpr const char s_pIndexProperty[] = "index";

    if (json.HasMember(s_pMaterialsProperty) && json[s_pMaterialsProperty].IsArray())
    {
        Value &materials = json[s_pMaterialsProperty];
        SizeType materialCount = materials.Size();

        for (SizeType i = 0; i < materialCount; ++i)
        {
            Value &material = materials[i];
            if (material.HasMember(s_pPBRProperty) && material[s_pPBRProperty].IsObject())
            {
                Value &pbr = material[s_pPBRProperty];
                if (pbr.HasMember(s_pBaseTextureProperty) && pbr[s_pBaseTextureProperty].IsObject())
                {
                    Value &baseTexture = pbr[s_pBaseTextureProperty];
                    if (baseTexture.HasMember(s_pIndexProperty) && baseTexture[s_pIndexProperty].IsInt())
                    {
                        Value &indexProperty = baseTexture[s_pIndexProperty];
                        int index = indexProperty.GetInt() + 1; // +1 since sqlite integer primary keys start at 1

                        if (!InsertMaterialDataEntry(index))
                        {
                            return false;
                        }
                    }
                }
            }
        }
    }

    return true;
}

bool AssetDatabaseBuilder::InsertBufferViewMetadata(Document &json)
{
    
}

bool AssetDatabaseBuilder::InsertMetadata(Document &json)
{
    // Mesh
    // SubMesh
    // SubMeshVertexStreams

    return 
        InsertMaterialMetadata(json) &&
        InsertBufferViewMetadata(json);
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
                    BuildMeshes(json, pSrcRootPath, pDstRootPath) &&
                    InsertMetadata(json);
            }
        }
    }

    ReleaseResources();

    return success;
}
