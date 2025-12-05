/*
 *
 * Copyright 2025 gRPC authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package credentials

import (
	"crypto/tls"
	"crypto/x509"
	"testing"
)

func TestValidateAuthority(t *testing.T) {
	// Create a dummy certificate for "example.com"
	cert := &x509.Certificate{
		DNSNames: []string{"example.com"},
	}
	// Add the certificate to a ConnectionState
	state := tls.ConnectionState{
		PeerCertificates: []*x509.Certificate{cert},
	}
	// Create a TLSInfo using the ConnectionState
	tlsInfo := TLSInfo{State: state}

	// Test case 1: Authority with port
	err := tlsInfo.ValidateAuthority("example.com:443")
	if err != nil {
		t.Errorf("ValidateAuthority failed for 'example.com:443': %v", err)
	}

	// Test case 2: Authority without port
	err = tlsInfo.ValidateAuthority("example.com")
	if err != nil {
		t.Errorf("ValidateAuthority failed for 'example.com': %v", err)
	}

	// Test case 3: Invalid Authority
	err = tlsInfo.ValidateAuthority("invalid.com")
	if err == nil {
		t.Errorf("ValidateAuthority succeeded for 'invalid.com', expected failure")
	}

    // Test case 4: Invalid Authority with port
	err = tlsInfo.ValidateAuthority("invalid.com:443")
	if err == nil {
		t.Errorf("ValidateAuthority succeeded for 'invalid.com:443', expected failure")
	}
}
