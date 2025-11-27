package main

import (
    "github.com/robomotionio/robomotion-go/runtime"

    "github.com/example/robomotion-seatable/v1"
)

func main() {
    runtime.RegisterNodes(
        &v1.SeaTableConnect{},
        &v1.SeaTableSQLQuery{},
        &v1.SeaTableRows{},
        &v1.SeaTableRowsGetMany{},
        &v1.SeaTableSearch{},
        &v1.SeaTableGetRow{},
        &v1.SeaTableUploadAttachment{},
        &v1.SeaTableLink{},
        &v1.SeaTableAutoLink{},
        // New nodes
        &v1.SeaTableGetMetadata{},
        &v1.SeaTableListColumns{},
        &v1.SeaTableListViews{},
        &v1.SeaTableDownloadFile{},
    )
    runtime.Start()
}
