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

// Next should be called by a component in order to pass the message to the next
// component in the pipeline.
type Next func()

// Done should be called by a component in order to return the message to the
// message handler. Can be used by the last component to inform that the
// message processing is done o by a middle component to inform an error.
type Done func(*Message, int, string)

// Composer represents a component in the pipeline that performs a work on
// a message
type Composer interface {
	Spawn(int) Composer
	OnMessage(*Message, Done)
	Workers() int
}
