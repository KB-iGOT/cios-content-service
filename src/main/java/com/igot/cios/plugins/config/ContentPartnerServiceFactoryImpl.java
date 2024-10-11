package com.igot.cios.plugins.config;

import com.igot.cios.plugins.ContentSource;
import com.igot.cios.plugins.ContentPartnerPluginService;
import com.igot.cios.plugins.cornell.CornellPluginServiceImpl;
import com.igot.cios.plugins.coursera.CourseraPluginServiceImpl;
import com.igot.cios.plugins.upgrad.UpgradPluginServiceImpl;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class ContentPartnerServiceFactoryImpl implements ContentPartnerServiceFactory {

    private final CornellPluginServiceImpl cornellPluginService;
    private final UpgradPluginServiceImpl upgradPluginService;
    private final CourseraPluginServiceImpl courseraPluginService;

    @Autowired
    public ContentPartnerServiceFactoryImpl(CornellPluginServiceImpl cornellPluginService,
                                            UpgradPluginServiceImpl upgradPluginService,CourseraPluginServiceImpl courseraPluginService) {
        this.cornellPluginService = cornellPluginService;
        this.upgradPluginService = upgradPluginService;
        this.courseraPluginService = courseraPluginService;
    }

    @Override
    public ContentPartnerPluginService getContentPartnerPluginService(ContentSource contentSource) {
        switch (contentSource) {
            case CORNELL:
                return cornellPluginService;
            case UPGRAD:
                return upgradPluginService;
            case COURSERA:
                return courseraPluginService;
            default:
                throw new IllegalArgumentException("Unsupported ContentSource: " + contentSource);
        }
    }
}

