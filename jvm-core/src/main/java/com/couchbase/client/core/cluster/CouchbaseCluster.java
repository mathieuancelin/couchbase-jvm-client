/**
 * Copyright (C) 2014 Couchbase, Inc.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
 * FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALING
 * IN THE SOFTWARE.
 */

package com.couchbase.client.core.cluster;

import com.couchbase.client.core.config.Configuration;
import com.couchbase.client.core.config.ConfigurationManager;
import com.couchbase.client.core.config.DefaultConfigurationManager;
import com.couchbase.client.core.environment.CouchbaseEnvironment;
import com.couchbase.client.core.environment.Environment;
import com.couchbase.client.core.message.CouchbaseRequest;
import com.couchbase.client.core.message.CouchbaseResponse;
import com.couchbase.client.core.message.common.CommonRequest;
import com.couchbase.client.core.message.common.CommonResponse;
import com.couchbase.client.core.message.common.ConnectRequest;
import com.couchbase.client.core.message.common.ConnectResponse;
import com.couchbase.client.core.message.internal.AddNodeRequest;
import com.couchbase.client.core.message.internal.AddNodeResponse;
import com.couchbase.client.core.message.internal.AddServiceRequest;
import com.couchbase.client.core.message.internal.AddServiceResponse;
import com.couchbase.client.core.message.internal.InternalRequest;
import com.couchbase.client.core.message.internal.InternalResponse;
import com.couchbase.client.core.message.internal.RemoveNodeRequest;
import com.couchbase.client.core.message.internal.RemoveNodeResponse;
import com.couchbase.client.core.message.internal.RemoveServiceRequest;
import com.couchbase.client.core.message.internal.RemoveServiceResponse;
import com.couchbase.client.core.node.CouchbaseNode;
import com.couchbase.client.core.node.Node;
import com.couchbase.client.core.state.AbstractStateMachine;
import com.couchbase.client.core.state.LifecycleState;
import reactor.core.composable.Deferred;
import reactor.core.composable.Promise;
import reactor.core.composable.spec.Promises;
import reactor.event.registry.CachingRegistry;
import reactor.event.registry.Registration;
import reactor.event.registry.Registry;
import reactor.function.Consumer;

import java.net.InetSocketAddress;
import java.util.List;

import static reactor.event.selector.Selectors.$;

/**
 * The default implementation of a {@link Cluster}.
 *
 * TODO:
 *  - add service (check if node is in reg if not, add it), delegate to node
 *  - test add service
 *
 *  - remove service, delegate to node
 *  - test remove service
 *
 *  - attach streams to all connected nodes, react on changes and build the states for the cluster.
 *
 *  - implement mock test of config loading from connecting
 *
 *  - Implement basic bootstrapping in config manager (+ cccp)
 *
 *  - implement node layer
 *
 *  - implement service layer
 *
 *  - implement endpoint layer + netty transition
 *
 */
public class CouchbaseCluster extends AbstractStateMachine<LifecycleState> implements Cluster {

    private final Environment env;
    private final ConfigurationManager configurationManager;
    private final Registry<Node> nodeRegistry;

    CouchbaseCluster(final Environment env, final ConfigurationManager manager, final Registry<Node> registry) {
        super(LifecycleState.DISCONNECTED, env);
        this.env = env;
        configurationManager = manager;
        nodeRegistry = registry;
    }

    public CouchbaseCluster() {
        this(new CouchbaseEnvironment());
    }

	public CouchbaseCluster(final Environment env) {
		super(LifecycleState.DISCONNECTED, env);
        this.env = env;
        configurationManager = new DefaultConfigurationManager(env, this);
        nodeRegistry = new CachingRegistry<Node>();
	}

	@Override
	public Promise<? extends CouchbaseResponse> send(final CouchbaseRequest request) {
        if (request instanceof CommonRequest) {
            return dispatchCommon((CommonRequest) request);
        } else if (request instanceof InternalRequest) {
            return dispatchInternal((InternalRequest) request);
        } else {
			throw new UnsupportedOperationException("Unsupported CouchbaseRequest type: " + request);
		}
	}

    /**
     * Helper method to dispatch common messages.
     *
     * @param request
     * @return
     */
    private Promise<? extends CommonResponse> dispatchCommon(final CommonRequest request) {
        if (request instanceof ConnectRequest) {
            return handleConnect((ConnectRequest) request);
        } else {
            throw new UnsupportedOperationException("Unsupported CouchbaseRequest type: " + request);
        }
    }

    /**
     * Helper method to dispatch internal messages.
     *
     * @param request
     * @return
     */
    private Promise<? extends InternalResponse> dispatchInternal(final InternalRequest request) {
        if (request instanceof AddNodeRequest) {
            return handleAddNode((AddNodeRequest) request);
        } else if (request instanceof AddServiceRequest) {
            return handleAddService((AddServiceRequest) request);
        } else if (request instanceof RemoveNodeRequest) {
            return handleRemoveNode((RemoveNodeRequest) request);
        } else if (request instanceof RemoveServiceRequest) {
            return handleRemoveService((RemoveServiceRequest) request);
        } else {
            throw new UnsupportedOperationException("Unsupported CouchbaseRequest type: " + request);
        }
    }

    /**
     * Handle a {@link ConnectRequest}.
     *
     * @param request
     * @return
     */
	private Promise<ConnectResponse> handleConnect(final ConnectRequest request) {
        Promise<Configuration> connectPromise = configurationManager.connect(
            request.getSeedNodes(), request.getBucketName(), request.getBucketPassword()
        );

        final Deferred<ConnectResponse,Promise<ConnectResponse>> deferred = Promises.defer(
            env.reactorEnv(), reactor.core.Environment.THREAD_POOL
        );

        connectPromise.onComplete(new Consumer<Promise<Configuration>>() {
            @Override
            public void accept(final Promise<Configuration> configPromise) {
                if (configPromise.isSuccess()) {
                    deferred.accept(ConnectResponse.connected());
                } else {
                    deferred.accept(configPromise.reason());
                }
            }
        });

        return deferred.compose();
	}

    /**
     * Add a node if it isn't added already.
     *
     * @param request
     * @return
     */
    private Promise<AddNodeResponse> handleAddNode(final AddNodeRequest request) {
        InetSocketAddress address = request.getAddress();

        List<Registration<? extends Node>> registrations = nodeRegistry.select(address);
        if (registrations.isEmpty()) {
            Node node = new CouchbaseNode(env, address);
            nodeRegistry.register($(address), node);
        }
        return Promises.success(AddNodeResponse.nodeAdded()).get();
    }

    /**
     * Remove a node if one has been added with that selector.
     *
     * @param request
     * @return
     */
    private Promise<RemoveNodeResponse> handleRemoveNode(final RemoveNodeRequest request) {
        final Deferred<RemoveNodeResponse, Promise<RemoveNodeResponse>> deferredResponse =
            Promises.defer(env.reactorEnv());

        List<Registration<? extends Node>> registrations = nodeRegistry.select(request.getAddress());
        if (registrations.isEmpty()) {
            deferredResponse.accept(RemoveNodeResponse.nodeRemoved());
        } else {
            Node node = registrations.get(0).getObject();
            node.shutdown().onComplete(new Consumer<Promise<Boolean>>() {
                @Override
                public void accept(Promise<Boolean> shutdownPromise) {
                    if (shutdownPromise.isSuccess()) {
                        deferredResponse.accept(RemoveNodeResponse.nodeRemoved());
                    } else {
                        deferredResponse.accept(shutdownPromise.reason());
                    }
                }
            });
        }
        return deferredResponse.compose();
    }

    /**
     *
     * @param request
     * @return
     */
    private Promise<AddServiceResponse> handleAddService(final AddServiceRequest request) {
        // send add service request to node

        return null;
    }

    /**
     *
     * @param request
     * @return
     */
    private Promise<RemoveServiceResponse> handleRemoveService(final RemoveServiceRequest request) {
        // send remove service to node
        return null;
    }

}
