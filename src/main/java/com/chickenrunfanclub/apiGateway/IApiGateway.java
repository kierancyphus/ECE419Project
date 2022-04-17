package com.chickenrunfanclub.apiGateway;

import com.chickenrunfanclub.app_kvECS.AllServerMetadata;

public interface IApiGateway {
    public void replaceAllServerMetadata(AllServerMetadata asm);
    public void close();

    // Everything else should be handled in the connection class
}
