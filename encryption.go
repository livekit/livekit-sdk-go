package lksdk

import (
	"bytes"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"crypto/sha256"
	"errors"
	"io"

	"golang.org/x/crypto/hkdf"
	"golang.org/x/crypto/pbkdf2"
)

const (
	LIVEKIT_SDK_SALT         = "LKFrameEncryptionKey"
	LIVEKIT_IV_LENGTH        = 12
	LIVEKIT_PBKDF_ITERATIONS = 100000
	LIVEKIT_KEY_SIZE_BYTES   = 16
	LIVEKIT_HKDF_INFO_BYTES  = 128
	unencrypted_audio_bytes  = 1
)

var ErrIncorrectKeyLength = errors.New("incorrect key length for encryption/decryption")
var ErrUnableGenerateIV = errors.New("unable to generate iv for encryption")
var ErrIncorrectIVLength = errors.New("incorrect iv length")
var ErrIncorrectSecretLength = errors.New("input secret provided to derivation function cannot be empty or nil")
var ErrIncorrectSaltLength = errors.New("input salt provided to derivation function cannot be empty or nil")

func DeriveKeyFromString(password string) ([]byte, error) {
	return DeriveKeyFromStringCustomSalt(password, LIVEKIT_SDK_SALT)
}

func DeriveKeyFromStringCustomSalt(password, salt string) ([]byte, error) {

	if password == "" {
		return nil, ErrIncorrectSecretLength
	}
	if salt == "" {
		return nil, ErrIncorrectSaltLength
	}

	encPassword := []byte(password)
	encSalt := []byte(salt)

	return pbkdf2.Key(encPassword, encSalt, LIVEKIT_PBKDF_ITERATIONS, LIVEKIT_KEY_SIZE_BYTES, sha256.New), nil

}

func DeriveKeyFromBytes(secret []byte) ([]byte, error) {
	return DeriveKeyFromBytesCustomSalt(secret, LIVEKIT_SDK_SALT)
}

func DeriveKeyFromBytesCustomSalt(secret []byte, salt string) ([]byte, error) {

	info := make([]byte, LIVEKIT_HKDF_INFO_BYTES)
	encSalt := []byte(salt)

	if secret == nil {
		return nil, ErrIncorrectSecretLength
	}
	if salt == "" {
		return nil, ErrIncorrectSaltLength
	}

	hkdfReader := hkdf.New(sha256.New, secret, encSalt, info)

	key := make([]byte, LIVEKIT_KEY_SIZE_BYTES)
	_, err := io.ReadFull(hkdfReader, key)
	if err != nil {
		return nil, err
	}

	return key, nil

}

// Take audio sample (body of RTP) encrypted by LiveKit client SDK, extract IV and decrypt using provided key
// Encrypted sample format based on livekit client sdk
// ---------+-------------------------+---------+----
// payload  |IV...(length = IV_LENGTH)|IV_LENGTH|KID|
// ---------+-------------------------+---------+----
// First byte of audio frame is not encrypted and only authenticated
// payload - variable bytes
// IV - variable bytes (equal to IV_LENGTH bytes)
// IV_LENGTH - 1 byte
// KID (Key ID) - 1 byte - ignored here, key is provided as parameter to function
func DecryptGCMAudioSample(sample, key, sifTrailer []byte) ([]byte, error) {

	if len(key) != 16 {
		return nil, ErrIncorrectKeyLength
	}

	if sifTrailer != nil && len(sample) >= len(sifTrailer) {
		possibleTrailer := sample[len(sample)-len(sifTrailer):]
		if bytes.Equal(possibleTrailer, sifTrailer) {
			// this is unencrypted Server Injected Frame (SIF) thas should be dropped
			return nil, nil
		}

	}

	// variable naming is kept close to LiveKit client SDK decrypt function
	// https://github.com/livekit/client-sdk-js/blob/main/src/e2ee/worker/FrameCryptor.ts#L402

	frameHeader := sample[:unencrypted_audio_bytes] // first unencrypted bytes are "frameHeader" and used for authentication later
	frameTrailer := sample[len(sample)-2:]          // last 2 bytes having IV_LENGTH and KID (1 byte each)
	ivLength := int(frameTrailer[0])                // single byte, Endianness doesn't matter
	ivStart := len(sample) - len(frameTrailer) - ivLength
	if ivStart < 0 {
		return nil, ErrIncorrectIVLength
	}

	iv := make([]byte, ivLength)
	copy(iv, sample[ivStart:ivStart+ivLength]) // copy IV value out of sample into iv

	cipherTextStart := len(frameHeader)
	cipherTextLength := len(sample) - len(frameTrailer) - ivLength - len(frameHeader)
	cipherText := make([]byte, cipherTextLength)
	copy(cipherText, sample[cipherTextStart:cipherTextStart+cipherTextLength])

	// setup AES
	aesCipher, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}

	aesGCM, err := cipher.NewGCMWithNonceSize(aesCipher, ivLength) // standard Nonce size is 12 bytes, but since it MAY be different in the sample, we use the one from the sample
	if err != nil {
		return nil, err
	}

	// fmt.Println("**** DECRYPTION BEGIN ********")
	plainText, err := aesGCM.Open(nil, iv, cipherText, frameHeader)
	if err != nil {
		return nil, err
	}

	newData := make([]byte, len(frameHeader)+len(plainText)) // allocate space for final packet

	_ = copy(newData[0:], frameHeader)              // put unencrypted frameHeader first
	_ = copy(newData[len(frameHeader):], plainText) // add decrypted remaining value

	return newData, nil

}

// Take audio sample (body of RTP) and encrypts it using AES-GCM 128bit with provided key
// Encrypted sample format based on livekit client sdk
// ---------+-------------------------+---------+----
// payload  |IV...(length = IV_LENGTH)|IV_LENGTH|KID|
// ---------+-------------------------+---------+----
// First byte of audio frame is not encrypted and only authenticated
// payload - variable bytes
// IV - variable bytes (equal to IV_LENGTH bytes) - 12 random bytes
// IV_LENGTH - 1 byte - 12 bytes fixed
// KID (Key ID) - 1 byte - taken from "kid" parameter
func EncryptGCMAudioSample(sample, key []byte, kid uint8) ([]byte, error) {

	if len(key) != 16 {
		return nil, ErrIncorrectKeyLength
	}

	// variable naming is kept close to LiveKit client SDK decrypt function
	// https://github.com/livekit/client-sdk-js/blob/main/src/e2ee/worker/FrameCryptor.ts#L402

	frameHeader := append(make([]byte, 0), sample[:unencrypted_audio_bytes]...) // first unencrypted bytes are "frameHeader" and used for authentication later
	iv := make([]byte, LIVEKIT_IV_LENGTH)
	_, err := rand.Read(iv)
	if err != nil {
		return nil, errors.Join(ErrUnableGenerateIV, err)
	}

	frameTrailer := []byte{LIVEKIT_IV_LENGTH, kid} // last 2 bytes having IV_LENGTH and KID (1 byte each)

	plainTextStart := len(frameHeader)
	plainTextLength := len(sample) - len(frameHeader)
	plainText := make([]byte, plainTextLength)
	copy(plainText, sample[plainTextStart:plainTextStart+plainTextLength])

	// setup AES
	aesCipher, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}

	aesGCM, err := cipher.NewGCMWithNonceSize(aesCipher, LIVEKIT_IV_LENGTH) // standard Nonce size is 12 bytes, but using one from defined constant (which matches Javascript SDK)
	if err != nil {
		return nil, err
	}

	cipherText := aesGCM.Seal(nil, iv, plainText, frameHeader)

	newData := make([]byte, len(frameHeader)+len(cipherText)+len(iv)+len(frameTrailer)) // allocate space for final packet

	_ = copy(newData[0:], frameHeader)                                         // put unencrypted frameHeader first
	_ = copy(newData[len(frameHeader):], cipherText)                           // add cipherText
	_ = copy(newData[len(frameHeader)+len(cipherText):], iv)                   // add iv
	_ = copy(newData[len(frameHeader)+len(cipherText)+len(iv):], frameTrailer) // add trailer

	return newData, nil

}
