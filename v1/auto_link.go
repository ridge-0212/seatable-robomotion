package v1

import (
    "context"
    "encoding/json"
    "fmt"
    "strings"

    "github.com/robomotionio/robomotion-go/message"
    "github.com/robomotionio/robomotion-go/runtime"
)

// SeaTableAutoLink automatically links rows between two tables based on key columns.
type SeaTableAutoLink struct {
    runtime.Node `spec:"id=Robomotion.SeaTable.AutoLink,name=Auto Link,icon=mdiLinkPlus,color=#00C2E0,inputs=1,outputs=1"`

    InClientID       runtime.InVariable[string] `spec:"title=Client ID,type=string,scope=Message,name=clientId,messageScope,jsScope,customScope"`
    InTableName      runtime.InVariable[string] `spec:"title=Table Name (left),type=string,scope=Message,name=tableName,messageScope,jsScope,customScope"`
    InOtherTableName runtime.InVariable[string] `spec:"title=Other Table Name (right),type=string,scope=Message,name=otherTableName,messageScope,jsScope,customScope"`
    InLinkID         runtime.InVariable[string] `spec:"title=Link ID,type=string,scope=Message,name=linkId,messageScope,jsScope,customScope"`
    InLeftKeyColumn  runtime.InVariable[string] `spec:"title=Left Key Column,type=string,scope=Message,name=leftKeyColumn,messageScope,jsScope,customScope"`
    InRightKeyColumn runtime.InVariable[string] `spec:"title=Right Key Column,type=string,scope=Message,name=rightKeyColumn,messageScope,jsScope,customScope"`

    OptMode        string                    `spec:"title=Mode,value=override,enum=override,enumNames=Override,option"`
    OptMaxLeftRows runtime.OptVariable[int]  `spec:"title=Max Left Rows,type=int,value=1000,scope=Message,name=maxLeftRows,messageScope,customScope,jsScope"`
    OptMaxRightRows runtime.OptVariable[int] `spec:"title=Max Right Rows,type=int,value=1000,scope=Message,name=maxRightRows,messageScope,customScope,jsScope"`
    OptDryRun       runtime.OptVariable[bool] `spec:"title=Dry Run,type=bool,value=false,scope=Message,name=dryRun,messageScope,customScope,jsScope"`

    OutProcessedLeftRows runtime.OutVariable[int]    `spec:"title=Processed Left Rows,type=int,scope=Message,name=processedLeftRows,messageScope"`
    OutMatchedRows       runtime.OutVariable[int]    `spec:"title=Matched Left Rows,type=int,scope=Message,name=matchedLeftRows,messageScope"`
    OutCreatedLinks      runtime.OutVariable[int]    `spec:"title=Created Links,type=int,scope=Message,name=createdLinks,messageScope"`
    OutSkippedRows       runtime.OutVariable[int]    `spec:"title=Skipped (no match),type=int,scope=Message,name=skippedRows,messageScope"`
    OutMode              runtime.OutVariable[string] `spec:"title=Mode Used,type=string,scope=Message,name=mode,messageScope"`
}

func (n *SeaTableAutoLink) OnCreate() error { return nil }
func (n *SeaTableAutoLink) OnClose() error  { return nil }

func (n *SeaTableAutoLink) OnMessage(ctx message.Context) error {
    clientID, err := n.InClientID.Get(ctx)
    if err != nil {
        return err
    }
    cfg, ok := getSeaTableClient(clientID)
    if !ok {
        return runtime.NewError("ErrInvalidArg", "Unknown Client ID – run SeaTable.Connect first")
    }

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
    linkID, err := n.InLinkID.Get(ctx)
    if err != nil {
        return err
    }
    linkID = strings.TrimSpace(linkID)
    leftKeyCol, err := n.InLeftKeyColumn.Get(ctx)
    if err != nil {
        return err
    }
    leftKeyCol = strings.TrimSpace(leftKeyCol)
    rightKeyCol, err := n.InRightKeyColumn.Get(ctx)
    if err != nil {
        return err
    }
    rightKeyCol = strings.TrimSpace(rightKeyCol)

    if tableName == "" || otherTableName == "" || linkID == "" || leftKeyCol == "" || rightKeyCol == "" {
        return runtime.NewError("ErrInvalidArg", "Table, Other Table, Link ID and key columns are required")
    }

    maxLeft, _ := n.OptMaxLeftRows.Get(ctx)
    if maxLeft <= 0 {
        maxLeft = 1000
    }
    maxRight, _ := n.OptMaxRightRows.Get(ctx)
    if maxRight <= 0 {
        maxRight = 1000
    }
    dryRun, _ := n.OptDryRun.Get(ctx)

    goCtx := context.Background()

    // Fetch right table map[key] -> []row_id
    rightRows, err := fetchRowsForKey(goCtx, cfg, otherTableName, rightKeyCol, maxRight)
    if err != nil {
        return fmt.Errorf("fetch right table rows: %w", err)
    }
    rightIndex := make(map[string][]string)
    for _, r := range rightRows {
        keyVal := getStringFromRow(r, rightKeyCol)
        if keyVal == "" {
            continue
        }
        rid := getStringFromRow(r, "_id")
        if rid == "" {
            continue
        }
        rightIndex[keyVal] = append(rightIndex[keyVal], rid)
    }

    leftRows, err := fetchRowsForKey(goCtx, cfg, tableName, leftKeyCol, maxLeft)
    if err != nil {
        return fmt.Errorf("fetch left table rows: %w", err)
    }

    processed := 0
    matched := 0
    skipped := 0
    created := 0

    for _, row := range leftRows {
        processed++
        leftRowID := getStringFromRow(row, "_id")
        if leftRowID == "" {
            skipped++
            continue
        }
        keyVal := getStringFromRow(row, leftKeyCol)
        if keyVal == "" {
            skipped++
            continue
        }
        targets, ok := rightIndex[keyVal]
        if !ok || len(targets) == 0 {
            skipped++
            continue
        }
        matched++

        if dryRun {
            created += len(targets)
            continue
        }

        url := fmt.Sprintf("%s/api-gateway/api/v2/dtables/%s/links/", cfg.Server, cfg.BaseUUID)
        body := map[string]any{
            "link_id":          linkID,
            "table_name":       tableName,
            "other_table_name": otherTableName,
            "row_id":           leftRowID,
            "other_rows_ids":   targets,
        }
        _, _, err := doSeaTableRequest(goCtx, "PUT", url, cfg.Token, body)
        if err != nil {
            return fmt.Errorf("update links for row %s: %w", leftRowID, err)
        }
        created += len(targets)
    }

    n.OutProcessedLeftRows.Set(ctx, processed)
    n.OutMatchedRows.Set(ctx, matched)
    n.OutSkippedRows.Set(ctx, skipped)
    n.OutCreatedLinks.Set(ctx, created)
    n.OutMode.Set(ctx, "override")
    return nil
}

// fetchRowsForKey uses SQL API to fetch _id and keyColumn.
func fetchRowsForKey(ctx context.Context, cfg *SeaTableClient, tableName, keyColumn string, limit int) ([]map[string]any, error) {
    sqlText := fmt.Sprintf("SELECT _id, %s FROM %s WHERE %s IS NOT NULL LIMIT %d", keyColumn, tableName, keyColumn, limit)
    url := fmt.Sprintf("%s/api-gateway/api/v2/dtables/%s/sql/", cfg.Server, cfg.BaseUUID)
    body := map[string]any{
        "sql":          sqlText,
        "convert_keys": true,
    }
    respBody, _, err := doSeaTableRequest(ctx, "POST", url, cfg.Token, body)
    if err != nil {
        return nil, err
    }

    var parsed struct {
        Results []map[string]any `json:"results"`
    }
    if err := json.Unmarshal(respBody, &parsed); err == nil && parsed.Results != nil {
        return parsed.Results, nil
    }

    // fallback
    var alt map[string]any
    if err := json.Unmarshal(respBody, &alt); err != nil {
        return nil, err
    }
    if arr, ok := alt["results"].([]any); ok {
        rows := make([]map[string]any, 0, len(arr))
        for _, v := range arr {
            if m, ok := v.(map[string]any); ok {
                rows = append(rows, m)
            }
        }
        return rows, nil
    }
    return nil, fmt.Errorf("unexpected SQL response for fetchRowsForKey")
}

func getStringFromRow(row map[string]any, key string) string {
    if row == nil {
        return ""
    }
    v, ok := row[key]
    if !ok || v == nil {
        return ""
    }
    switch t := v.(type) {
    case string:
        return t
    case float64:
        s := fmt.Sprintf("%v", t)
        if strings.HasSuffix(s, ".0") {
            s = strings.TrimSuffix(s, ".0")
        }
        return s
    default:
        return fmt.Sprintf("%v", t)
    }
}
