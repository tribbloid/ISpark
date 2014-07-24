package org.tribbloid.ispark

package object msg {
    type MIME = String

    type Data = Map[MIME, String]
    val Data = Map

    type Metadata = Map[String, String]
    val Metadata = Map

    type MsgType = MsgType.Value

    type ExecutionStatus = ExecutionStatus.Value
    type HistAccessType = HistAccessType.Value
    type ExecutionState = ExecutionState.Value
}
