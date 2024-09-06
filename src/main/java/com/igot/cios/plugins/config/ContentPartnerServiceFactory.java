package com.igot.cios.plugins.config;

import com.igot.cios.plugins.ContentSource;
import com.igot.cios.plugins.ContentPartnerPluginService;

public interface ContentPartnerServiceFactory {
    ContentPartnerPluginService getContentPartnerPluginService(ContentSource contentSource);
}
