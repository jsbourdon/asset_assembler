#include <pch.h>
#include "AssetDatabaseBuilder.h"
#include "Salvation_Common/Memory/ThreadHeapAllocator.h"
#include "Salvation_Common/Memory/ThreadHeapSmartPointer.h"
#include "Salvation_Common/FileSystem/FileSystem.h"
#include "Salvation_Common/Assets/AssetDatabase.h"
#include "Salvation_Common/sqlite/sqlite3.h"
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
    ReleaseUpdateStatements();

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
    static constexpr char s_BufferViewStr[] = "INSERT INTO BufferView(BufferID, ByteSize, ByteOffset, Stride) VALUES(?1, ?2, ?3, ?4);";
    static constexpr char s_MeshStr[] = "INSERT INTO Mesh(Name) VALUES(?1);";
    static constexpr char s_SubMeshStr[] = "INSERT INTO SubMesh(MeshID, IndexBufferID, MaterialID) VALUES(?1, ?2, ?3);";
    static constexpr char s_VertexStreamStr[] = "INSERT INTO SubMeshVertexStreams(SubMeshID, BufferViewID, Attribute) VALUES(?1, ?2, ?3);";

    return
        sqlite3_prepare_v2(m_pDb, s_PackedDataStr, -1, &m_InsertStmts.m_pPackedDataStmt, nullptr) == SQLITE_OK &&
        sqlite3_prepare_v2(m_pDb, s_TextureStr, -1, &m_InsertStmts.m_pTextureStmt, nullptr) == SQLITE_OK &&
        sqlite3_prepare_v2(m_pDb, s_BufferStr, -1, &m_InsertStmts.m_pBufferStmt, nullptr) == SQLITE_OK &&
        sqlite3_prepare_v2(m_pDb, s_MaterialStr, -1, &m_InsertStmts.m_pMaterialStmt, nullptr) == SQLITE_OK &&
        sqlite3_prepare_v2(m_pDb, s_BufferViewStr, -1, &m_InsertStmts.m_pBufferViewStmt, nullptr) == SQLITE_OK &&
        sqlite3_prepare_v2(m_pDb, s_MeshStr, -1, &m_InsertStmts.m_pMeshStmt, nullptr) == SQLITE_OK &&
        sqlite3_prepare_v2(m_pDb, s_SubMeshStr, -1, &m_InsertStmts.m_pSubMeshStmt, nullptr) == SQLITE_OK && 
        sqlite3_prepare_v2(m_pDb, s_VertexStreamStr, -1, &m_InsertStmts.m_pVertexStreamStmt, nullptr) == SQLITE_OK;
}

bool AssetDatabaseBuilder::CreateUpdateStatements()
{
    static constexpr char s_PackedDataStr[] = "UPDATE PackedData SET ByteSize = ?2 WHERE ID = ?1;";

    return
        sqlite3_prepare_v2(m_pDb, s_PackedDataStr, -1, &m_UpdateStmts.m_pPackedDataStmt, nullptr) == SQLITE_OK;
}


void AssetDatabaseBuilder::ReleaseInsertStatements()
{
    if (m_InsertStmts.m_pPackedDataStmt) sqlite3_finalize(m_InsertStmts.m_pPackedDataStmt);
    if (m_InsertStmts.m_pTextureStmt) sqlite3_finalize(m_InsertStmts.m_pTextureStmt);
    if (m_InsertStmts.m_pBufferStmt) sqlite3_finalize(m_InsertStmts.m_pBufferStmt);
    if (m_InsertStmts.m_pMaterialStmt) sqlite3_finalize(m_InsertStmts.m_pMaterialStmt);
    if (m_InsertStmts.m_pBufferViewStmt) sqlite3_finalize(m_InsertStmts.m_pBufferViewStmt);
    if (m_InsertStmts.m_pMeshStmt) sqlite3_finalize(m_InsertStmts.m_pMeshStmt);
    if (m_InsertStmts.m_pSubMeshStmt) sqlite3_finalize(m_InsertStmts.m_pSubMeshStmt);
    if (m_InsertStmts.m_pVertexStreamStmt) sqlite3_finalize(m_InsertStmts.m_pVertexStreamStmt);
}

void AssetDatabaseBuilder::ReleaseUpdateStatements()
{
    if (m_UpdateStmts.m_pPackedDataStmt) sqlite3_finalize(m_UpdateStmts.m_pPackedDataStmt);
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
        DataType INTEGER NOT NULL,
        ByteSize INTEGER
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
        Stride INTEGER NOT NULL,
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
    int64_t byteSize = 0;

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
    sqlite3_stmt *pStmt = m_InsertStmts.m_pPackedDataStmt;

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
    sqlite3_stmt *pStmt = m_InsertStmts.m_pTextureStmt;

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
    sqlite3_stmt *pStmt = m_InsertStmts.m_pBufferStmt;

    return
        sqlite3_reset(pStmt) == SQLITE_OK &&
        sqlite3_bind_int64(pStmt, 1, byteSize) == SQLITE_OK &&
        sqlite3_bind_int64(pStmt, 2, byteOffset) == SQLITE_OK &&
        sqlite3_bind_int64(pStmt, 3, packedDataId) == SQLITE_OK &&
        sqlite3_step(pStmt) == SQLITE_DONE;
}

bool AssetDatabaseBuilder::InsertMaterialDataEntry(int64_t textureId)
{
    sqlite3_stmt *pStmt = m_InsertStmts.m_pMaterialStmt;

    return
        sqlite3_reset(pStmt) == SQLITE_OK &&
        sqlite3_bind_int64(pStmt, 1, textureId) == SQLITE_OK &&
        sqlite3_step(pStmt) == SQLITE_DONE;
}

bool AssetDatabaseBuilder::InsertBufferViewDataEntry(int64_t bufferId, int64_t byteSize, int64_t byteOffset, int32_t stride)
{
    sqlite3_stmt *pStmt = m_InsertStmts.m_pBufferViewStmt;

    return
        sqlite3_reset(pStmt) == SQLITE_OK &&
        sqlite3_bind_int64(pStmt, 1, bufferId) == SQLITE_OK &&
        sqlite3_bind_int64(pStmt, 2, byteSize) == SQLITE_OK &&
        sqlite3_bind_int64(pStmt, 3, byteOffset) == SQLITE_OK &&
        sqlite3_bind_int(pStmt, 4, stride) == SQLITE_OK &&
        sqlite3_step(pStmt) == SQLITE_DONE;
}

int64_t AssetDatabaseBuilder::InsertMeshDataEntry(const char *pName)
{
    int64_t meshId = -1;
    sqlite3_stmt *pStmt = m_InsertStmts.m_pMeshStmt;

    if (
        sqlite3_reset(pStmt) == SQLITE_OK &&
        sqlite3_bind_text(pStmt, 1, pName, -1, SQLITE_STATIC) == SQLITE_OK &&
        sqlite3_step(pStmt) == SQLITE_DONE)
    {
        meshId = sqlite3_last_insert_rowid(m_pDb);
    }

    return meshId;
}

int64_t AssetDatabaseBuilder::InsertSubMeshDataEntry(int64_t meshId, int64_t indexBufferViewId, int64_t materialId)
{
    int64_t subMeshId = -1;
    sqlite3_stmt *pStmt = m_InsertStmts.m_pSubMeshStmt;

    if (
        sqlite3_reset(pStmt) == SQLITE_OK &&
        sqlite3_bind_int64(pStmt, 1, meshId) == SQLITE_OK &&
        sqlite3_bind_int64(pStmt, 2, indexBufferViewId) == SQLITE_OK &&
        sqlite3_bind_int64(pStmt, 3, materialId) == SQLITE_OK &&
        sqlite3_step(pStmt) == SQLITE_DONE)
    {
        subMeshId = sqlite3_last_insert_rowid(m_pDb);
    }

    return subMeshId;
}

bool AssetDatabaseBuilder::InsertVertexStreamDataEntry(int64_t subMeshId, int64_t bufferViewId, int32_t attribute)
{
    sqlite3_stmt *pStmt = m_InsertStmts.m_pVertexStreamStmt;

    return
        sqlite3_reset(pStmt) == SQLITE_OK &&
        sqlite3_bind_int64(pStmt, 1, subMeshId) == SQLITE_OK &&
        sqlite3_bind_int64(pStmt, 2, bufferViewId) == SQLITE_OK &&
        sqlite3_bind_int(pStmt, 3, attribute) == SQLITE_OK &&
        sqlite3_step(pStmt) == SQLITE_DONE;
}

bool AssetDatabaseBuilder::UpdatePackagedDataEntry(int64_t packagedDataId, int64_t byteSize)
{
    sqlite3_stmt *pStmt = m_UpdateStmts.m_pPackedDataStmt;

    return
        sqlite3_reset(pStmt) == SQLITE_OK &&
        sqlite3_bind_int64(pStmt, 1, packagedDataId) == SQLITE_OK &&
        sqlite3_bind_int64(pStmt, 2, byteSize) == SQLITE_OK &&
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

            if (!UpdatePackagedDataEntry(packedDataId, currentByteOffset))
            {
                return false;
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

            if (writeSucceeded && !UpdatePackagedDataEntry(packedDataId, currentByteOffset))
            {
                return false;
            }

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

ComponentType AssetDatabaseBuilder::GetComponentType(const char *pGLTFType, int glTFComponentType)
{
    static constexpr uint32_t s_glTFByteCode = 5120;
    static constexpr uint32_t s_glTFUnsignedByteCode = 5121;
    static constexpr uint32_t s_glTFShortCode = 5122;
    static constexpr uint32_t s_glTFUnsignedShortCode = 5123;
    static constexpr uint32_t s_glTUnsignedIntCode = 5125;
    static constexpr uint32_t s_glTFFloatCode = 5126;

    static constexpr const char *s_pGLTFVectorTypes[] =
    {
        "VEC2",
        "VEC3",
        "VEC4",
        "MAT2",
        "MAT3",
        "MAT4"
    };

    static constexpr ComponentType s_VectorTypes[] =
    {
        ComponentType::Vec2,
        ComponentType::Vec3,
        ComponentType::Vec4,
        ComponentType::Matrix2x2,
        ComponentType::Matrix3x3,
        ComponentType::Matrix4x4
    };

    if (strcmp(pGLTFType, "SCALAR") == 0)
    {
        switch (glTFComponentType)
        {
        case s_glTFByteCode:
        case s_glTFUnsignedByteCode:
            return ComponentType::Scalar_Byte;
        case s_glTFShortCode:
        case s_glTFUnsignedShortCode:
            return ComponentType::Scalar_Short;
        case s_glTUnsignedIntCode:
            return ComponentType::Scalar_Int;
        case s_glTFFloatCode:
            return ComponentType::Scalar_Float;
        default:
            return ComponentType::Unknown;
        }
    }

    for (size_t i = 0; i < ARRAY_SIZE(s_pGLTFVectorTypes); ++i)
    {
        if (strcmp(pGLTFType, s_pGLTFVectorTypes[i]) == 0)
        {
            return s_VectorTypes[i];
        }
    }

    return ComponentType::Unknown;
}

bool AssetDatabaseBuilder::InsertBufferViewMetadata(Document &json)
{
    static constexpr const char s_pAccessorsProperty[] = "accessors";
    static constexpr const char s_pBufferViewsProperty[] = "bufferViews";
    static constexpr const char s_pBufferViewProperty[] = "bufferView";
    static constexpr const char s_pByteOffsetProperty[] = "byteOffset";
    static constexpr const char s_pComponentTypeProperty[] = "componentType";
    static constexpr const char s_pTypeProperty[] = "type";
    static constexpr const char s_pCountProperty[] = "count";
    static constexpr const char s_pBufferProperty[] = "buffer";

    if (
        json.HasMember(s_pAccessorsProperty) && json[s_pAccessorsProperty].IsArray() &&
        json.HasMember(s_pBufferViewsProperty) && json[s_pBufferViewsProperty].IsArray())
    {
        Value &bufferViews = json[s_pBufferViewsProperty];
        Value &accessors = json[s_pAccessorsProperty];
        SizeType accessorCount = accessors.Size();

        for (SizeType i = 0; i < accessorCount; ++i)
        {
            Value &accessor = accessors[i];
            if (accessor.HasMember(s_pBufferViewProperty) && accessor[s_pBufferViewProperty].IsInt())
            {
                int bufferViewIndex = accessor[s_pBufferViewProperty].GetInt();
                Value &bufferView = bufferViews[bufferViewIndex];

                if (
                    accessor.HasMember(s_pTypeProperty) && accessor[s_pTypeProperty].IsString() &&
                    accessor.HasMember(s_pCountProperty) && accessor[s_pCountProperty].IsInt() &&
                    accessor.HasMember(s_pComponentTypeProperty) && accessor[s_pComponentTypeProperty].IsInt() &&
                    bufferView.HasMember(s_pBufferProperty) && bufferView[s_pBufferProperty].IsInt())
                {
                    int64_t bufferId = bufferView[s_pBufferProperty].GetInt() + 1; // +1 since sqlite integer primary keys start at 1

                    const char *pType = accessor[s_pTypeProperty].GetString();
                    int glTFComponentType = accessor[s_pComponentTypeProperty].GetInt();
                    int count = accessor[s_pCountProperty].GetInt();

                    ComponentType componentType = GetComponentType(pType, glTFComponentType);
                    int32_t stride = AssetDatabase::ComponentTypeByteSize(componentType);
                    int64_t byteSize = stride * static_cast<int64_t>(count);

                    int64_t accessorByteOffset = 0;
                    int64_t bufferViewByteOffset = 0;

                    if (accessor.HasMember(s_pByteOffsetProperty) && accessor[s_pByteOffsetProperty].IsInt())
                    {
                        accessorByteOffset = accessor[s_pByteOffsetProperty].GetInt();
                    }

                    if (bufferView.HasMember(s_pByteOffsetProperty) && bufferView[s_pByteOffsetProperty].IsInt())
                    {
                        bufferViewByteOffset = bufferView[s_pByteOffsetProperty].GetInt();
                    }

                    int64_t byteOffset = accessorByteOffset + bufferViewByteOffset;

                    if (!InsertBufferViewDataEntry(bufferId, byteSize, byteOffset, stride))
                    {
                        return false;
                    }
                }
            }
        }
    }


    return true;
}

bool AssetDatabaseBuilder::InsertMeshMetadata(Document &json)
{
    static constexpr const char s_pMeshesProperty[] = "meshes";
    static constexpr const char s_pPrimitivesProperty[] = "primitives";
    static constexpr const char s_pIndicesProperty[] = "indices";
    static constexpr const char s_pMaterialProperty[] = "material";
    static constexpr const char s_pAttributesProperty[] = "attributes";

    if (json.HasMember(s_pMeshesProperty) && json[s_pMeshesProperty].IsArray())
    {
        Value &meshes = json[s_pMeshesProperty];
        SizeType meshCount = meshes.Size();

        for (SizeType meshIndex = 0; meshIndex < meshCount; ++meshIndex)
        {
            Value &mesh = meshes[meshIndex];

            if (mesh.HasMember(s_pPrimitivesProperty) && mesh[s_pPrimitivesProperty].IsArray())
            {
                Value &primitives = mesh[s_pPrimitivesProperty];
                SizeType primCount = primitives.Size();

                for (SizeType primIndex = 0; primIndex < primCount; ++primIndex)
                {
                    Value &primitive = primitives[primIndex];

                    if (
                        primitive.HasMember(s_pIndicesProperty) && primitive[s_pIndicesProperty].IsInt() &&
                        primitive.HasMember(s_pMaterialProperty) && primitive[s_pMaterialProperty].IsInt() &&
                        primitive.HasMember(s_pAttributesProperty) && primitive[s_pAttributesProperty].IsObject())
                    {
                        // +1 since sqlite integer primary keys start at 1
                        int64_t indexBufferId = primitive[s_pIndicesProperty].GetInt() + 1;
                        int64_t materialId = primitive[s_pMaterialProperty].GetInt() + 1;

                        int64_t meshId = InsertMeshDataEntry("Default Name");
                        int64_t subMeshId = InsertSubMeshDataEntry(meshId, indexBufferId, materialId);

                        Value &attributes = primitive[s_pAttributesProperty];

                        if (meshId < 0 || subMeshId < 0 || !InsertVertexStreamsMetadata(attributes, subMeshId))
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

bool AssetDatabaseBuilder::InsertVertexStreamsMetadata(Value &attributes, int64_t subMeshId)
{
    static constexpr const char *s_ppAttributeSemantics[] =
    {
        "POSITION",
        "NORMAL",
        "TANGENT",
        "TEXCOORD_0",
        "TEXCOORD_1",
        "COLOR_0"
    };

    static_assert(ARRAY_SIZE(s_ppAttributeSemantics) == static_cast<size_t>(AttributeSemantic::Count));

    for (size_t i = 0; i < ARRAY_SIZE(s_ppAttributeSemantics); ++i)
    {
        const char *pAttributeSemantic = s_ppAttributeSemantics[i];
        if (attributes.HasMember(pAttributeSemantic) && attributes[pAttributeSemantic].IsInt())
        {
            int bufferViewId = attributes[pAttributeSemantic].GetInt() + 1; // +1 since sqlite integer primary keys start at 1
            if (!InsertVertexStreamDataEntry(subMeshId, bufferViewId, static_cast<int32_t>(i)))
            {
                return false;
            }
        }
    }

    return true;
}

bool AssetDatabaseBuilder::InsertMetadata(Document &json)
{
    return 
        InsertMaterialMetadata(json) &&
        InsertBufferViewMetadata(json) && 
        InsertMeshMetadata(json);
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
                    CreateUpdateStatements() &&
                    BuildTextures(json, pSrcRootPath, pDstRootPath) &&
                    BuildMeshes(json, pSrcRootPath, pDstRootPath) &&
                    InsertMetadata(json);
            }
        }
    }

    ReleaseResources();

    return success;
}
