package org.tribbloid.ispark

package object msg {

    type Metadata = Map[String, String]
    val Metadata = Map

    type MsgType = MsgTypes.Value
    type ExecutionStatus = ExecutionStatuses.Value
    type HistAccessType = HistAccessTypes.Value
    type ExecutionState = ExecutionStates.Value
}
