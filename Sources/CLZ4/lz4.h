#ifndef CLZ4_SHIM_H
#define CLZ4_SHIM_H

#ifdef __cplusplus
extern "C" {
#endif

int LZ4_compress_default(const char* src, char* dst, int srcSize, int dstCapacity);
int LZ4_decompress_safe(const char* src, char* dst, int compressedSize, int dstCapacity);
int LZ4_compressBound(int inputSize);

#ifdef __cplusplus
}
#endif

#endif /* CLZ4_SHIM_H */
