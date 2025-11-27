package v1

import (
    "bytes"
    "context"
    "encoding/json"
    "fmt"
    "io"
    "mime/multipart"
    "net/http"
    "os"
    "path/filepath"
    "strings"
    "time"

    "github.com/robomotionio/robomotion-go/message"
    "github.com/robomotionio/robomotion-go/runtime"
)

type uploadLinkResponse struct {
    UploadLink        string `json:"upload_link"`
    ParentPath        string `json:"parent_path"`
    FileRelativePath  string `json:"file_relative_path"`
    ImageRelativePath string `json:"image_relative_path"`
}

// SeaTableUploadAttachment uploads a file and returns attachment info.
type SeaTableUploadAttachment struct {
    runtime.Node `spec:"id=Robomotion.SeaTable.UploadAttachment,name=Upload Attachment,icon=mdiPaperclip,color=#00C2E0,inputs=1,outputs=1"`

    InClientID runtime.InVariable[string] `spec:"title=Client ID,type=string,scope=Message,name=clientId,messageScope,jsScope,customScope"`
    InFilePath runtime.InVariable[string] `spec:"title=File Path,type=string,scope=Message,name=filePath,messageScope,jsScope,customScope"`

    OptFileName runtime.OptVariable[string] `spec:"title=File Name (override),type=string,scope=Message,name=fileName,messageScope,customScope,jsScope"`
    OptKind     runtime.OptVariable[string] `spec:"title=Kind,value=file,enum=file|image,enumNames=File|Image,option,scope=Message,name=kind,messageScope,customScope,jsScope"`

    OutAttachment   runtime.OutVariable[any]    `spec:"title=Attachment Object,type=object,scope=Message,name=attachment,messageScope"`
    OutRelativePath runtime.OutVariable[string] `spec:"title=Relative Path,type=string,scope=Message,name=relativePath,messageScope"`
}

func (n *SeaTableUploadAttachment) OnCreate() error { return nil }
func (n *SeaTableUploadAttachment) OnClose() error  { return nil }

func (n *SeaTableUploadAttachment) OnMessage(ctx message.Context) error {
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
    if strings.TrimSpace(filePath) == "" {
        return runtime.NewError("ErrInvalidArg", "File Path is required")
    }

    fileName, _ := n.OptFileName.Get(ctx)
    kind, _ := n.OptKind.Get(ctx)
    if kind == "" {
        kind = "file"
    }

    goCtx := context.Background()

    linkResp, err := getUploadLink(goCtx, cfg)
    if err != nil {
        return err
    }

    attachment, rel, err := uploadFileWithLink(goCtx, cfg, linkResp, filePath, fileName, kind)
    if err != nil {
        return err
    }

    n.OutAttachment.Set(ctx, attachment)
    n.OutRelativePath.Set(ctx, rel)
    return nil
}

func getUploadLink(ctx context.Context, cfg *SeaTableClient) (*uploadLinkResponse, error) {
    url := fmt.Sprintf("%s/api/v2.1/dtable/app-upload-link/", cfg.Server)
    body, status, err := doSeaTableRequest(ctx, "GET", url, cfg.Token, nil)
    if err != nil {
        return nil, err
    }
    if status >= 300 {
        return nil, fmt.Errorf("get upload link failed: status=%d body=%s", status, string(body))
    }
    var out uploadLinkResponse
    if err := json.Unmarshal(body, &out); err != nil {
        return nil, err
    }
    if out.UploadLink == "" {
        return nil, fmt.Errorf("upload_link is empty")
    }
    return &out, nil
}

func uploadFileWithLink(
    ctx context.Context,
    cfg *SeaTableClient,
    link *uploadLinkResponse,
    filePath, fileName, kind string,
) (map[string]any, string, error) {
    if fileName == "" {
        fileName = filepath.Base(filePath)
    }

    f, err := os.Open(filePath)
    if err != nil {
        return nil, "", fmt.Errorf("open file: %w", err)
    }
    defer f.Close()

    var buf bytes.Buffer
    mw := multipart.NewWriter(&buf)

    fw, err := mw.CreateFormFile("file", fileName)
    if err != nil {
        return nil, "", err
    }
    if _, err := io.Copy(fw, f); err != nil {
        return nil, "", err
    }
    if err := mw.WriteField("parent_dir", link.ParentPath); err != nil {
        return nil, "", err
    }
    if err := mw.Close(); err != nil {
        return nil, "", err
    }

    uploadURL := fmt.Sprintf("%s/seafhttp/upload-api/%s?ret-json=1", cfg.Server, link.UploadLink)
    req, err := http.NewRequestWithContext(ctx, "POST", uploadURL, &buf)
    if err != nil {
        return nil, "", err
    }
    req.Header.Set("Content-Type", mw.FormDataContentType())

    client := &http.Client{Timeout: 60 * time.Second}
    resp, err := client.Do(req)
    if err != nil {
        return nil, "", err
    }
    defer resp.Body.Close()

    respBody, err := io.ReadAll(resp.Body)
    if err != nil {
        return nil, "", err
    }
    if resp.StatusCode >= 300 {
        return nil, "", fmt.Errorf("upload failed: status=%d body=%s", resp.StatusCode, string(respBody))
    }

    var arr []map[string]any
    if err := json.Unmarshal(respBody, &arr); err != nil {
        return nil, "", err
    }
    if len(arr) == 0 {
        return nil, "", fmt.Errorf("no attachment returned")
    }

    var rel string
    if kind == "image" && link.ImageRelativePath != "" {
        rel = link.ImageRelativePath
    } else {
        rel = link.FileRelativePath
    }

    return arr[0], rel, nil
}
