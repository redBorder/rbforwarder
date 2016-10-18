// Copyright (C) ENEO Tecnologia SL - 2016
//
// Authors: Diego Fernández Barrera <dfernandez@redborder.com> <bigomby@gmail.com>
// 					Eugenio Pérez Martín <eugenio@redborder.com>
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with this program.  If not, see <https://www.gnu.org/licenses/lgpl-3.0.txt>.

package utils

import (
	"errors"

	"github.com/oleiade/lane"
	"github.com/streamrail/concurrent-map"
)

// Message is used to send data through the pipeline
type Message struct {
	Opts    cmap.ConcurrentMap
	Reports *lane.Stack

	payload *lane.Stack
}

// NewMessage creates a new instance of Message
func NewMessage() *Message {
	return &Message{
		payload: lane.NewStack(),
		Reports: lane.NewStack(),
		Opts:    cmap.New(),
	}
}

// PushPayload store data on an LIFO queue so the nexts handlers can use it
func (m *Message) PushPayload(data []byte) {
	m.payload.Push(data)
}

// PopPayload get the data stored by the previous handler
func (m *Message) PopPayload() (data []byte, err error) {
	if m.payload.Empty() {
		err = errors.New("No payload available")
		return
	}

	data = m.payload.Pop().([]byte)

	return
}
