package v1

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/robomotionio/robomotion-go/message"
	"github.com/robomotionio/robomotion-go/runtime"
)

// SeaTableDownloadFile downloads a file/image from SeaTable and saves it locally.
type SeaTableDownloadFile struct {
	runtime.Node `spec:"id=Robomotion.SeaTable.DownloadFile,name=Download File,icon=mdiDownload,color=#00C2E0,inputs=1,outputs=1"`

	InClientID runtime.InVariable[string] `spec:"title=Client ID,type=string,scope=Message,name=clientId,messageScope,jsScope,customScope"`
	InFilePath runtime.InVariable[string] `spec:"title=File Path (in SeaTable),type=string,scope=Message,name=filePath,messageScope,jsScope,customScope"`

	OptSavePath runtime.OptVariable[string] `spec:"title=Save Path (local),type=string,scope=Message,name=savePath,messageScope,customScope,jsScope"`

	OutStatusCode   runtime.OutVariable[int]    `spec:"title=Status Code,type=int,scope=Message,name=statusCode,messageScope"`
	OutDownloadURL  runtime.OutVariable[string] `spec:"title=Download URL,type=string,scope=Message,name=downloadUrl,messageScope"`
	OutSavedPath    runtime.OutVariable[string] `spec:"title=Saved Path,type=string,scope=Message,name=savedPath,messageScope"`
	OutFileSize     runtime.OutVariable[int]    `spec:"title=File Size (bytes),type=int,scope=Message,name=fileSize,messageScope"`
}

func (n *SeaTableDownloadFile) OnCreate() error { return nil }
func (n *SeaTableDownloadFile) OnClose() error  { return nil }

func (n *SeaTableDownloadFile) OnMessage(ctx message.Context) error {
	clientID, err := n.InClientID.Get(ctx)
	if err != nil {
		return err
	}
	cfg, ok := getSeaTableClient(clientID)
	if !ok {
		return runtime.NewError("ErrInvalidArg", "Unknown Client ID – run SeaTable.Connect first")
	}

	filePath, err := n.InFilePath.Get(ctx)
	if err != nil {
		return err
	}
	filePath = strings.TrimSpace(filePath)
	if filePath == "" {
		return runtime.NewError("ErrInvalidArg", "File Path is required")
	}

	savePath, _ := n.OptSavePath.Get(ctx)
	savePath = strings.TrimSpace(savePath)

	goCtx := context.Background()

	// Step 1: Get download link
	downloadURL, err := getDownloadLink(goCtx, cfg, filePath)
	if err != nil {
		return err
	}

	n.OutDownloadURL.Set(ctx, downloadURL)

	// Step 2: Download the file if savePath is provided
	if savePath != "" {
		fileSize, err := downloadAndSaveFile(goCtx, downloadURL, savePath)
		if err != nil {
			return err
		}
		n.OutSavedPath.Set(ctx, savePath)
		n.OutFileSize.Set(ctx, fileSize)
		n.OutStatusCode.Set(ctx, 200)
	} else {
		// Just return the download URL without downloading
		n.OutSavedPath.Set(ctx, "")
		n.OutFileSize.Set(ctx, 0)
		n.OutStatusCode.Set(ctx, 200)
	}

	return nil
}

func getDownloadLink(ctx context.Context, cfg *SeaTableClient, filePath string) (string, error) {
	// The API endpoint for getting download link
	url := fmt.Sprintf("%s/api/v2.1/dtable/app-download-link/?path=%s", cfg.Server, filePath)
	
	respBody, status, err := doSeaTableRequest(ctx, "GET", url, cfg.Token, nil)
	if err != nil {
		return "", err
	}
	if status >= 300 {
		return "", fmt.Errorf("get download link failed: status=%d body=%s", status, string(respBody))
	}

	var result map[string]any
	if err := json.Unmarshal(respBody, &result); err != nil {
		return "", fmt.Errorf("parse download link response: %w", err)
	}

	downloadLink, ok := result["download_link"].(string)
	if !ok || downloadLink == "" {
		return "", fmt.Errorf("download_link not found in response")
	}

	return downloadLink, nil
}

func downloadAndSaveFile(ctx context.Context, downloadURL, savePath string) (int, error) {
	// Ensure directory exists
	dir := filepath.Dir(savePath)
	if dir != "" && dir != "." {
		if err := os.MkdirAll(dir, 0755); err != nil {
			return 0, fmt.Errorf("create directory: %w", err)
		}
	}

	// Create HTTP request
	req, err := http.NewRequestWithContext(ctx, "GET", downloadURL, nil)
	if err != nil {
		return 0, fmt.Errorf("create download request: %w", err)
	}

	client := &http.Client{Timeout: 5 * time.Minute}
	resp, err := client.Do(req)
	if err != nil {
		return 0, fmt.Errorf("download request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 300 {
		return 0, fmt.Errorf("download failed with status: %d", resp.StatusCode)
	}

	// Create output file
	outFile, err := os.Create(savePath)
	if err != nil {
		return 0, fmt.Errorf("create output file: %w", err)
	}
	defer outFile.Close()

	// Copy content
	written, err := io.Copy(outFile, resp.Body)
	if err != nil {
		return 0, fmt.Errorf("save file: %w", err)
	}

	return int(written), nil
}

