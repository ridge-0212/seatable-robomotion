package v1

import (
    "context"
    "encoding/json"
    "fmt"
    "strings"

    "github.com/robomotionio/robomotion-go/message"
    "github.com/robomotionio/robomotion-go/runtime"
)

// SeaTableLink manages record links between two tables.
type SeaTableLink struct {
    runtime.Node `spec:"id=Robomotion.SeaTable.Link,name=Link Records,icon=mdiLinkVariant,color=#00C2E0,inputs=1,outputs=1"`

    InClientID       runtime.InVariable[string] `spec:"title=Client ID,type=string,scope=Message,name=clientId,messageScope,jsScope,customScope"`
    InOperation      runtime.InVariable[string] `spec:"title=Operation (add|update|remove),type=string,scope=Message,name=operation,messageScope,jsScope,customScope"`
    InLinkID         runtime.InVariable[string] `spec:"title=Link ID,type=string,scope=Message,name=linkId,messageScope,jsScope,customScope"`
    InTableName      runtime.InVariable[string] `spec:"title=Table Name,type=string,scope=Message,name=tableName,messageScope,jsScope,customScope"`
    InOtherTableName runtime.InVariable[string] `spec:"title=Other Table Name,type=string,scope=Message,name=otherTableName,messageScope,jsScope,customScope"`
    InRowID          runtime.InVariable[string] `spec:"title=Row ID,type=string,scope=Message,name=rowId,messageScope,jsScope,customScope"`

    OptOtherRowID  runtime.OptVariable[string] `spec:"title=Other Row ID (for add/remove),type=string,scope=Message,name=otherRowId,messageScope,customScope,jsScope"`
    OptOtherRowIDs runtime.OptVariable[string] `spec:"title=Other Row IDs (for update),type=string,scope=Message,name=otherRowIds,messageScope,customScope,jsScope"`

    OutStatusCode runtime.OutVariable[int]    `spec:"title=Status Code,type=int,scope=Message,name=statusCode,messageScope"`
    OutRaw        runtime.OutVariable[string] `spec:"title=Raw Body,type=string,scope=Message,name=body,messageScope"`
    OutJSON       runtime.OutVariable[any]    `spec:"title=JSON,type=object,scope=Message,name=json,messageScope"`
}

func (n *SeaTableLink) OnCreate() error { return nil }
func (n *SeaTableLink) OnClose() error  { return nil }

func (n *SeaTableLink) OnMessage(ctx message.Context) error {
    clientID, err := n.InClientID.Get(ctx)
    if err != nil {
        return err
    }
    cfg, ok := getSeaTableClient(clientID)
    if !ok {
        return runtime.NewError("ErrInvalidArg", "Unknown Client ID – run SeaTable.Connect first")
    }

    op, err := n.InOperation.Get(ctx)
    if err != nil {
        return err
    }
    op = strings.ToLower(strings.TrimSpace(op))

    linkID, err := n.InLinkID.Get(ctx)
    if err != nil {
        return err
    }
    linkID = strings.TrimSpace(linkID)
    tableName, err := n.InTableName.Get(ctx)
    if err != nil {
        return err
    }
    tableName = strings.TrimSpace(tableName)
    otherTableName, err := n.InOtherTableName.Get(ctx)
    if err != nil {
        return err
    }
    otherTableName = strings.TrimSpace(otherTableName)
    rowID, err := n.InRowID.Get(ctx)
    if err != nil {
        return err
    }
    rowID = strings.TrimSpace(rowID)

    if linkID == "" || tableName == "" || otherTableName == "" || rowID == "" {
        return runtime.NewError("ErrInvalidArg", "Link ID, Table, Other Table and Row ID are required")
    }

    url := fmt.Sprintf("%s/api-gateway/api/v2/dtables/%s/links/", cfg.Server, cfg.BaseUUID)
    var method string
    var payload map[string]any

    switch op {
    case "add":
        otherRowID, _ := n.OptOtherRowID.Get(ctx)
        otherRowID = strings.TrimSpace(otherRowID)
        if otherRowID == "" {
            return runtime.NewError("ErrInvalidArg", "Other Row ID is required for add")
        }
        method = "POST"
        payload = map[string]any{
            "link_id":            linkID,
            "table_name":         tableName,
            "other_table_name":   otherTableName,
            "table_row_id":       rowID,
            "other_table_row_id": otherRowID,
        }

    case "remove":
        otherRowID, _ := n.OptOtherRowID.Get(ctx)
        otherRowID = strings.TrimSpace(otherRowID)
        if otherRowID == "" {
            return runtime.NewError("ErrInvalidArg", "Other Row ID is required for remove")
        }
        method = "DELETE"
        payload = map[string]any{
            "link_id":            linkID,
            "table_name":         tableName,
            "other_table_name":   otherTableName,
            "table_row_id":       rowID,
            "other_table_row_id": otherRowID,
        }

    case "update":
        raw, _ := n.OptOtherRowIDs.Get(ctx)
        raw = strings.TrimSpace(raw)
        if raw == "" {
            return runtime.NewError("ErrInvalidArg", "Other Row IDs is required for update")
        }
        ids, err := parseRowIDs(raw)
        if err != nil {
            return runtime.NewError("ErrInvalidArg", fmt.Sprintf("parse Other Row IDs: %v", err))
        }
        method = "PUT"
        payload = map[string]any{
            "link_id":          linkID,
            "table_name":       tableName,
            "other_table_name": otherTableName,
            "row_id":           rowID,
            "other_rows_ids":   ids,
        }

    default:
        return runtime.NewError("ErrInvalidArg", "Operation must be add, update or remove")
    }

    respBody, status, err := doSeaTableRequest(context.Background(), method, url, cfg.Token, payload)
    if err != nil {
        return err
    }

    n.OutStatusCode.Set(ctx, status)
    n.OutRaw.Set(ctx, string(respBody))

    var parsed any
    if err := json.Unmarshal(respBody, &parsed); err == nil {
        n.OutJSON.Set(ctx, parsed)
    }
    return nil
}

func parseRowIDs(s string) ([]string, error) {
    if s == "" {
        return []string{}, nil
    }
    s = strings.TrimSpace(s)
    var ids []string
    if strings.HasPrefix(s, "[") {
        if err := json.Unmarshal([]byte(s), &ids); err != nil {
            return nil, err
        }
    } else {
        parts := strings.Split(s, ",")
        for _, p := range parts {
            v := strings.TrimSpace(p)
            if v != "" {
                ids = append(ids, v)
            }
        }
    }
    return ids, nil
}
